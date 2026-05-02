#!/usr/bin/env python3
"""
Comprehensive test suite for streaming use cases matching pandas and polars behavior.
Tests cover: user session analytics, real-time fraud detection, IoT sensor processing, log analysis.
"""

import pytest
import numpy as np
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import sys
import os
import time
import gc
from typing import Dict, List, Tuple, Any

# Add the vectrill module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))

try:
    import vectrill
    from vectrill.dataframe import VectrillDataFrame, col, functions, window
    VECTRILL_AVAILABLE = True
except ImportError as e:
    pytest.skip(f"vectrill module not available: {e}", allow_module_level=True)
    VECTRILL_AVAILABLE = False


class TestUserSessionAnalytics:
    """Test user session analytics streaming use case"""
    
    @pytest.fixture
    def session_data(self):
        """Generate realistic user session data"""
        np.random.seed(42)
        
        # Generate session data with proper ordering
        num_sessions = 1000
        events_per_session = np.random.poisson(5, num_sessions) + 1
        
        data = []
        for session_id in range(1, num_sessions + 1):
            num_events = events_per_session[session_id - 1]
            session_start = pd.Timestamp('2023-01-01') + pd.Timedelta(days=np.random.randint(0, 30))
            
            for event_idx in range(num_events):
                data.append({
                    'timestamp': session_start + pd.Timedelta(seconds=event_idx * 5),
                    'session_id': session_id,
                    'user_id': np.random.randint(1, 50000),
                    'event_type': np.random.choice(['page_view', 'click', 'add_to_cart', 'purchase', 'logout'], 
                                                 p=[0.4, 0.3, 0.15, 0.1, 0.05]),
                    'page_url': f'/page_{np.random.randint(1, 100)}',
                    'revenue': np.random.exponential(25) * np.random.choice([0, 1], p=[0.85, 0.15]),
                    'device_type': np.random.choice(['mobile', 'desktop', 'tablet']),
                    'response_time_ms': np.random.exponential(150)
                })
        
        return pd.DataFrame(data).sort_values('timestamp')
    
    def test_session_duration_calculation(self, session_data):
        """Test session duration calculation across all libraries"""
        # Pandas implementation
        pandas_result = session_data.copy()
        session_times = pandas_result.groupby('session_id')['timestamp'].agg(['min', 'max'])
        session_times['duration_seconds'] = (session_times['max'] - session_times['min']).dt.total_seconds()
        pandas_result = pandas_result.merge(session_times['duration_seconds'], left_on='session_id', right_index=True)
        
        # Polars implementation
        polars_df = pl.DataFrame(session_data)
        polars_result = polars_df.with_columns([
            pl.col('timestamp').max().over('session_id').alias('session_end'),
            pl.col('timestamp').min().over('session_id').alias('session_start')
        ]).with_columns([
            ((pl.col('session_end') - pl.col('session_start')).dt.total_seconds()).alias('duration_seconds')
        ]).drop(['session_start', 'session_end'])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(session_data)
        vectrill_result = vectrill_df.with_columns([
            functions.max(col('timestamp')).over(window.partition_by('session_id')).alias('session_end'),
            functions.min(col('timestamp')).over(window.partition_by('session_id')).alias('session_start')
        ]).with_columns([
            (col('session_end') - col('session_start')).alias('duration_seconds')
        ]).select(['timestamp', 'session_id', 'user_id', 'event_type', 'page_url', 'revenue', 
                  'device_type', 'response_time_ms', 'duration_seconds'])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Convert timestamps for comparison
        pandas_result['duration_seconds'] = pandas_result['duration_seconds'].astype(float)
        vectrill_pd['duration_seconds'] = vectrill_pd['duration_seconds'].astype(float)
        
        # Sort by session_id and timestamp for comparison
        pandas_sorted = pandas_result.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        
        np.testing.assert_allclose(
            pandas_sorted['duration_seconds'].values,
            vectrill_sorted['duration_seconds'].values,
            rtol=1e-10
        )
    
    def test_session_revenue_calculation(self, session_data):
        """Test cumulative revenue calculation within sessions"""
        # Pandas implementation
        pandas_result = session_data.copy()
        pandas_result = pandas_result.sort_values(['session_id', 'timestamp'])
        pandas_result['cumulative_revenue'] = pandas_result.groupby('session_id')['revenue'].cumsum()
        
        # Polars implementation
        polars_df = pl.DataFrame(session_data)
        polars_result = polars_df.sort(['session_id', 'timestamp']).with_columns([
            (pl.col('revenue').shift(1).fill_null(0) + pl.col('revenue')).alias('cumulative_revenue')
        ])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(session_data)
        vectrill_result = vectrill_df.sort(['session_id', 'timestamp']).with_columns([
            functions.sum(col('revenue')).over(window.partition_by('session_id').order_by('timestamp')).alias('cumulative_revenue')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        
        np.testing.assert_allclose(
            pandas_sorted['cumulative_revenue'].values,
            vectrill_sorted['cumulative_revenue'].values,
            rtol=1e-10
        )
    
    def test_session_event_count(self, session_data):
        """Test running event count within sessions"""
        # Pandas implementation
        pandas_result = session_data.copy()
        pandas_result = pandas_result.sort_values(['session_id', 'timestamp'])
        pandas_result['event_count'] = pandas_result.groupby('session_id').cumcount() + 1
        
        # Polars implementation
        polars_df = pl.DataFrame(session_data)
        polars_result = polars_df.sort(['session_id', 'timestamp']).with_columns([
            pl.int_range(pl.len()).over('session_id').alias('event_count') + 1
        ])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(session_data)
        vectrill_result = vectrill_df.sort(['session_id', 'timestamp']).with_columns([
            functions.count().over(window.partition_by('session_id').order_by('timestamp')).alias('event_count')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['session_id', 'timestamp']).reset_index(drop=True)
        
        np.testing.assert_array_equal(
            pandas_sorted['event_count'].values,
            vectrill_sorted['event_count'].values
        )


class TestRealTimeFraudDetection:
    """Test real-time fraud detection streaming use case"""
    
    @pytest.fixture
    def transaction_data(self):
        """Generate realistic transaction data for fraud detection"""
        np.random.seed(42)
        
        num_transactions = 10000
        data = []
        
        for i in range(num_transactions):
            # Generate transactions with some fraudulent patterns
            if np.random.random() < 0.05:  # 5% fraudulent transactions
                # Fraudulent pattern: rapid successive transactions
                base_time = pd.Timestamp('2023-01-01') + pd.Timedelta(hours=i)
                for j in range(np.random.randint(3, 8)):  # 3-7 rapid transactions
                    data.append({
                        'timestamp': base_time + pd.Timedelta(seconds=j * 10),
                        'transaction_id': len(data) + 1,
                        'user_id': np.random.randint(1, 1000),
                        'amount': np.random.exponential(100) + 50,
                        'merchant_id': np.random.randint(1, 500),
                        'location_id': np.random.randint(1, 100),
                        'is_fraud': 1
                    })
            else:
                # Normal transaction
                data.append({
                    'timestamp': pd.Timestamp('2023-01-01') + pd.Timedelta(hours=i),
                    'transaction_id': len(data) + 1,
                    'user_id': np.random.randint(1, 1000),
                    'amount': np.random.exponential(50) + 10,
                    'merchant_id': np.random.randint(1, 500),
                    'location_id': np.random.randint(1, 100),
                    'is_fraud': 0
                })
        
        return pd.DataFrame(data).sort_values('timestamp')
    
    def test_transaction_frequency_detection(self, transaction_data):
        """Test detection of high-frequency transactions"""
        # Pandas implementation
        pandas_result = transaction_data.copy()
        pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
        pandas_result['prev_timestamp'] = pandas_result.groupby('user_id')['timestamp'].shift(1)
        pandas_result['time_diff_seconds'] = (pandas_result['timestamp'] - pandas_result['prev_timestamp']).dt.total_seconds()
        pandas_result['time_diff_seconds'] = pandas_result['time_diff_seconds'].fillna(0)
        
        # Flag transactions within 1 minute of previous transaction
        pandas_result['rapid_transaction'] = (pandas_result['time_diff_seconds'] < 60).astype(int)
        
        # Polars implementation
        polars_df = pl.DataFrame(transaction_data)
        polars_result = polars_df.sort(['user_id', 'timestamp']).with_columns([
            pl.col('timestamp').shift(1).over('user_id').alias('prev_timestamp')
        ]).with_columns([
            ((pl.col('timestamp') - pl.col('prev_timestamp')).dt.total_seconds()).alias('time_diff_seconds')
        ]).with_columns([
            pl.col('time_diff_seconds').fill_null(0),
            (pl.col('time_diff_seconds') < 60).alias('rapid_transaction')
        ])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(transaction_data)
        vectrill_result = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
            functions.lag(col('timestamp'), 1).over(window.partition_by('user_id').order_by('timestamp')).alias('prev_timestamp')
        ]).with_columns([
            (col('timestamp') - col('prev_timestamp')).alias('time_diff_seconds'),
            functions.when(col('time_diff_seconds').is_null()).then(0).otherwise(col('time_diff_seconds')).alias('time_diff_seconds'),
            functions.when(col('time_diff_seconds') < 60).then(1).otherwise(0).alias('rapid_transaction')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Convert time_diff_seconds to float for comparison
        pandas_result['time_diff_seconds'] = pandas_result['time_diff_seconds'].astype(float)
        vectrill_pd['time_diff_seconds'] = vectrill_pd['time_diff_seconds'].astype(float)
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
        
        np.testing.assert_allclose(
            pandas_sorted['time_diff_seconds'].values,
            vectrill_sorted['time_diff_seconds'].values,
            rtol=1e-10
        )
        
        np.testing.assert_array_equal(
            pandas_sorted['rapid_transaction'].values,
            vectrill_sorted['rapid_transaction'].values
        )
    
    def test_amount_anomaly_detection(self, transaction_data):
        """Test detection of unusual transaction amounts"""
        # Pandas implementation
        pandas_result = transaction_data.copy()
        pandas_result = pandas_result.sort_values(['user_id', 'timestamp'])
        pandas_result['user_avg_amount'] = pandas_result.groupby('user_id')['amount'].transform('mean')
        pandas_result['amount_z_score'] = np.abs((pandas_result['amount'] - pandas_result['user_avg_amount']) / 
                                                pandas_result.groupby('user_id')['amount'].transform('std'))
        pandas_result['amount_anomaly'] = (pandas_result['amount_z_score'] > 3).astype(int)
        
        # Polars implementation
        polars_df = pl.DataFrame(transaction_data)
        polars_result = polars_df.sort(['user_id', 'timestamp']).with_columns([
            pl.col('amount').mean().over('user_id').alias('user_avg_amount'),
            pl.col('amount').std().over('user_id').alias('user_std_amount')
        ]).with_columns([
            ((pl.col('amount') - pl.col('user_avg_amount')) / pl.col('user_std_amount')).abs().alias('amount_z_score')
        ]).with_columns([
            (pl.col('amount_z_score') > 3).alias('amount_anomaly')
        ])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(transaction_data)
        vectrill_result = vectrill_df.sort(['user_id', 'timestamp']).with_columns([
            functions.mean(col('amount')).over(window.partition_by('user_id')).alias('user_avg_amount'),
            functions.std(col('amount')).over(window.partition_by('user_id')).alias('user_std_amount')
        ]).with_columns([
            functions.abs((col('amount') - col('user_avg_amount')) / col('user_std_amount')).alias('amount_z_score'),
            functions.when(col('amount_z_score') > 3).then(1).otherwise(0).alias('amount_anomaly')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['user_id', 'timestamp']).reset_index(drop=True)
        
        # Handle NaN values in z-score comparison
        pandas_z_score = np.nan_to_num(pandas_sorted['amount_z_score'].values, nan=0.0)
        vectrill_z_score = np.nan_to_num(vectrill_sorted['amount_z_score'].values, nan=0.0)
        
        np.testing.assert_allclose(pandas_z_score, vectrill_z_score, rtol=1e-10)
        np.testing.assert_array_equal(
            pandas_sorted['amount_anomaly'].values,
            vectrill_sorted['amount_anomaly'].values
        )


class TestIoTSensorProcessing:
    """Test IoT sensor data processing streaming use case"""
    
    @pytest.fixture
    def sensor_data(self):
        """Generate multi-sensor IoT data"""
        np.random.seed(42)
        
        num_sensors = 50
        measurements_per_sensor = 200
        data = []
        
        for sensor_id in range(1, num_sensors + 1):
            base_temp = 20 + np.random.normal(0, 5)
            base_humidity = 50 + np.random.normal(0, 10)
            
            for i in range(measurements_per_sensor):
                timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(minutes=i * 5)
                
                # Add realistic sensor patterns
                temp = base_temp + np.random.normal(0, 2) + 5 * np.sin(i * 0.1)
                humidity = base_humidity + np.random.normal(0, 5) - 10 * np.sin(i * 0.1)
                
                data.append({
                    'timestamp': timestamp,
                    'sensor_id': sensor_id,
                    'sensor_type': np.random.choice(['temperature', 'humidity', 'pressure']),
                    'location_id': np.random.randint(1, 10),
                    'temperature': temp if np.random.random() > 0.3 else None,
                    'humidity': humidity if np.random.random() > 0.3 else None,
                    'pressure': 1013 + np.random.normal(0, 5),
                    'battery_level': max(0, 100 - i * 0.01 + np.random.normal(0, 1))
                })
        
        return pd.DataFrame(data).sort_values(['sensor_id', 'timestamp'])
    
    def test_sensor_data_aggregation(self, sensor_data):
        """Test rolling aggregation of sensor data"""
        # Pandas implementation
        pandas_result = sensor_data.copy()
        pandas_result = pandas_result.sort_values(['sensor_id', 'timestamp'])
        pandas_result['temp_rolling_avg'] = pandas_result.groupby('sensor_id')['temperature'].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        )
        pandas_result['temp_rolling_std'] = pandas_result.groupby('sensor_id')['temperature'].transform(
            lambda x: x.rolling(window=5, min_periods=1).std()
        )
        
        # Polars implementation
        polars_df = pl.DataFrame(sensor_data)
        polars_result = polars_df.sort(['sensor_id', 'timestamp']).with_columns([
            pl.col('temperature').rolling_mean(window_size=5, min_periods=1).over('sensor_id').alias('temp_rolling_avg'),
            pl.col('temperature').rolling_std(window_size=5, min_periods=1).over('sensor_id').alias('temp_rolling_std')
        ])
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(sensor_data)
        vectrill_result = vectrill_df.sort(['sensor_id', 'timestamp']).with_columns([
            functions.rolling_mean(col('temperature'), window_size=5).over(window.partition_by('sensor_id').order_by('timestamp')).alias('temp_rolling_avg'),
            functions.rolling_std(col('temperature'), window_size=5).over(window.partition_by('sensor_id').order_by('timestamp')).alias('temp_rolling_std')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['sensor_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['sensor_id', 'timestamp']).reset_index(drop=True)
        
        # Handle NaN values in rolling statistics
        pandas_avg = np.nan_to_num(pandas_sorted['temp_rolling_avg'].values, nan=0.0)
        vectrill_avg = np.nan_to_num(vectrill_sorted['temp_rolling_avg'].values, nan=0.0)
        
        pandas_std = np.nan_to_num(pandas_sorted['temp_rolling_std'].values, nan=0.0)
        vectrill_std = np.nan_to_num(vectrill_sorted['temp_rolling_std'].values, nan=0.0)
        
        np.testing.assert_allclose(pandas_avg, vectrill_avg, rtol=1e-10)
        np.testing.assert_allclose(pandas_std, vectrill_std, rtol=1e-10)
    
    def test_sensor_anomaly_detection(self, sensor_data):
        """Test sensor anomaly detection using statistical methods"""
        # Pandas implementation
        pandas_result = sensor_data.copy()
        pandas_result = pandas_result.sort_values(['sensor_id', 'timestamp'])
        
        # Calculate baseline statistics for each sensor
        sensor_stats = pandas_result.groupby('sensor_id').agg({
            'temperature': ['mean', 'std'],
            'humidity': ['mean', 'std']
        }).reset_index()
        sensor_stats.columns = ['sensor_id', 'temp_mean', 'temp_std', 'humidity_mean', 'humidity_std']
        
        pandas_result = pandas_result.merge(sensor_stats, on='sensor_id')
        pandas_result['temp_anomaly'] = (np.abs(pandas_result['temperature'] - pandas_result['temp_mean']) > 
                                        3 * pandas_result['temp_std']).astype(int)
        pandas_result['humidity_anomaly'] = (np.abs(pandas_result['humidity'] - pandas_result['humidity_mean']) > 
                                           3 * pandas_result['humidity_std']).astype(int)
        
        # Vectrill implementation
        vectrill_df = vectrill.from_pandas(sensor_data)
        vectrill_result = vectrill_df.sort(['sensor_id', 'timestamp']).with_columns([
            functions.mean(col('temperature')).over(window.partition_by('sensor_id')).alias('temp_mean'),
            functions.std(col('temperature')).over(window.partition_by('sensor_id')).alias('temp_std'),
            functions.mean(col('humidity')).over(window.partition_by('sensor_id')).alias('humidity_mean'),
            functions.std(col('humidity')).over(window.partition_by('sensor_id')).alias('humidity_std')
        ]).with_columns([
            functions.when(functions.abs(col('temperature') - col('temp_mean')) > (3 * col('temp_std'))).then(1).otherwise(0).alias('temp_anomaly'),
            functions.when(functions.abs(col('humidity') - col('humidity_mean')) > (3 * col('humidity_std'))).then(1).otherwise(0).alias('humidity_anomaly')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['sensor_id', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['sensor_id', 'timestamp']).reset_index(drop=True)
        
        np.testing.assert_array_equal(
            pandas_sorted['temp_anomaly'].values,
            vectrill_sorted['temp_anomaly'].values
        )
        
        np.testing.assert_array_equal(
            pandas_sorted['humidity_anomaly'].values,
            vectrill_sorted['humidity_anomaly'].values
        )


class TestLogAnalysisPipeline:
    """Test log analysis pipeline streaming use case"""
    
    @pytest.fixture
    def log_data(self):
        """Generate realistic log data"""
        np.random.seed(42)
        
        num_logs = 5000
        data = []
        
        log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        services = ['auth', 'payment', 'user', 'inventory', 'notification']
        
        for i in range(num_logs):
            timestamp = pd.Timestamp('2023-01-01') + pd.Timedelta(seconds=i * 0.1)
            level = np.random.choice(log_levels, p=[0.1, 0.6, 0.2, 0.08, 0.02])
            
            # Create realistic log messages
            if level == 'ERROR':
                message = f"Error in {np.random.choice(services)}: {np.random.choice(['connection failed', 'timeout', 'invalid input'])}"
            elif level == 'FATAL':
                message = f"Fatal error in {np.random.choice(services)}: system crash"
            else:
                message = f"{np.random.choice(services)}: {np.random.choice(['request processed', 'cache hit', 'database query'])}"
            
            data.append({
                'timestamp': timestamp,
                'level': level,
                'service': np.random.choice(services),
                'message': message,
                'user_id': np.random.choice([None, np.random.randint(1, 10000)], p=[0.3, 0.7]),
                'request_id': f"req_{i:06d}",
                'response_time_ms': np.random.exponential(100) if level in ['INFO', 'DEBUG'] else None
            })
        
        return pd.DataFrame(data).sort_values('timestamp')
    
    def test_error_rate_calculation(self, log_data):
        """Test error rate calculation over time windows"""
        # Pandas implementation
        pandas_result = log_data.copy()
        pandas_result = pandas_result.sort_values('timestamp')
        pandas_result['is_error'] = pandas_result['level'].isin(['ERROR', 'FATAL']).astype(int)
        
        # Calculate rolling error rate (10-entry window for consistency)
        pandas_result['error_rate_1min'] = pandas_result['is_error'].rolling(
            window=10, min_periods=1
        ).mean()
        
        # Polars implementation
        polars_df = pl.DataFrame(log_data)
        polars_result = polars_df.sort('timestamp').with_columns([
            pl.col('level').is_in(['ERROR', 'FATAL']).cast(int).alias('is_error')
        ]).with_columns([
            pl.col('is_error').rolling_mean(window_size=10, min_periods=1).alias('error_rate_1min')
        ])
        
        # Vectrill implementation - simplified approach
        vectrill_df = vectrill.from_pandas(log_data)
        vectrill_df = vectrill_df.sort('timestamp')
        vectrill_result = vectrill_df.with_columns([
            functions.when(col('level').is_in(['ERROR', 'FATAL'])).then(1).otherwise(0).alias('is_error')
        ])
        # Apply rolling mean after sorting - this works with current implementation
        vectrill_result = vectrill_result.with_columns([
            functions.rolling_mean(col('is_error'), window_size=10).alias('error_rate_1min')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values('timestamp').reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values('timestamp').reset_index(drop=True)
        
        np.testing.assert_allclose(
            pandas_sorted['error_rate_1min'].values,
            vectrill_sorted['error_rate_1min'].values,
            rtol=1e-10
        )
    
    def test_service_performance_metrics(self, log_data):
        """Test service performance metrics calculation"""
        # Pandas implementation
        pandas_result = log_data.copy()
        pandas_result = pandas_result.sort_values(['service', 'timestamp'])
        
        # Calculate performance metrics per service
        service_metrics = pandas_result.groupby('service').agg({
            'response_time_ms': ['mean', 'median', 'std'],
            'level': lambda x: (x == 'ERROR').sum()
        }).reset_index()
        service_metrics.columns = ['service', 'avg_response_time', 'median_response_time', 'std_response_time', 'error_count']
        
        pandas_result = pandas_result.merge(service_metrics, on='service')
        
        # Vectrill implementation - simplified approach
        vectrill_df = vectrill.from_pandas(log_data)
        vectrill_df = vectrill_df.sort(['service', 'timestamp'])
        
        # Add error indicator column first
        vectrill_df = vectrill_df.with_columns([
            functions.when(col('level') == 'ERROR').then(1).otherwise(0).alias('is_error')
        ])
        
        # Then apply window functions
        vectrill_result = vectrill_df.with_columns([
            functions.mean(col('response_time_ms')).over(window.partition_by('service')).alias('avg_response_time'),
            functions.median(col('response_time_ms')).over(window.partition_by('service')).alias('median_response_time'),
            functions.std(col('response_time_ms')).over(window.partition_by('service')).alias('std_response_time'),
            # Use a simple approach for error count
            functions.sum(col('is_error')).over(window.partition_by('service')).alias('error_count')
        ])
        
        # Compare results
        vectrill_pd = vectrill_result.to_pandas()
        
        # Sort for comparison
        pandas_sorted = pandas_result.sort_values(['service', 'timestamp']).reset_index(drop=True)
        vectrill_sorted = vectrill_pd.sort_values(['service', 'timestamp']).reset_index(drop=True)
        
        # Handle NaN values in response time metrics
        for col_name in ['avg_response_time', 'median_response_time', 'std_response_time']:
            pandas_vals = np.nan_to_num(pandas_sorted[col_name].values, nan=0.0)
            vectrill_vals = np.nan_to_num(vectrill_sorted[col_name].values, nan=0.0)
            np.testing.assert_allclose(pandas_vals, vectrill_vals, rtol=1e-10)
        
        np.testing.assert_array_equal(
            pandas_sorted['error_count'].values,
            vectrill_sorted['error_count'].values
        )


if __name__ == "__main__":
    pytest.main([__file__])
