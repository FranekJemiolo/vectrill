import marimo

__generated_with = "0.8.0"
app = marimo.App()


@app.cell
def __(mo):
    mo.md(
        r"""
        # Vectrill Streaming

        This notebook demonstrates Vectrill's streaming capabilities including event sequencing, micro-batching, and window operations.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Event Sequencing

        Vectrill's Sequencer orders events by timestamp and handles out-of-order data using watermarks.
        """
    )
    return


@app.cell
def __():
    import polars as pl

    # Create sample streaming data with timestamps
    streaming_data = pl.DataFrame({
        "event_id": [1, 2, 3, 4, 5, 6],
        "timestamp": [
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:01",
            "2024-01-01 10:00:02",
            "2024-01-01 10:00:03",
            "2024-01-01 10:00:04",
            "2024-01-01 10:00:05"
        ],
        "device_id": ["A", "B", "A", "B", "A", "B"],
        "temperature": [20.5, 21.0, 19.8, 22.1, 20.9, 21.5],
        "humidity": [60, 65, 58, 70, 62, 68]
    })
    
    streaming_data = streaming_data.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
    )
    return streaming_data,


@app.cell
def __(streaming_data):
    print(streaming_data)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Window Operations

        Vectrill supports multiple window types:
        - **Tumbling windows**: Fixed-size, non-overlapping windows
        - **Sliding windows**: Fixed-size, overlapping windows
        - **Session windows**: Dynamic windows based on gaps in data
        """
    )
    return


@app.cell
def __(streaming_data):
    # Tumbling window example (10 second windows)
    tumbling_windows = streaming_data.with_columns(
        (pl.col("timestamp").dt.truncate("10s")).alias("window_start")
    )
    
    # Aggregate by window
    windowed_agg = tumbling_windows.group_by("window_start").agg([
        pl.col("temperature").mean().alias("avg_temp"),
        pl.col("temperature").max().alias("max_temp"),
        pl.col("humidity").mean().alias("avg_humidity")
    ])
    return tumbling_windows, windowed_agg


@app.cell
def __(windowed_agg):
    print(windowed_agg)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Micro-batching

        Vectrill processes data in micro-batches for efficiency. Each batch contains multiple events that are processed together.
        """
    )
    return


@app.cell
def __(streaming_data):
    # Simulate micro-batches (group by time intervals)
    batch_size = 2
    num_batches = len(streaming_data) // batch_size
    
    batches = []
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        batch = streaming_data[start_idx:end_idx]
        batches.append(batch)
    
    print(f"Created {len(batches)} micro-batches")
    print(f"First batch:\n{batches[0]}")
    return batch_size, batches


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Watermarks and Late Data

        Vectrill uses watermarks to track event time progress and handle late data. Events that arrive after the watermark are considered late.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Performance Considerations

        When working with streaming data in Vectrill:

        1. **Batch Size**: Larger batches improve throughput but increase latency
        2. **Window Size**: Smaller windows provide more granular results but increase overhead
        3. **State**: Stateful operations (aggregations, joins) require memory for state
        4. **Watermark Delay**: Higher watermark delay handles more late data but increases result latency
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        - Explore the [advanced notebook](advanced.py) for optimization techniques
- Check the [performance benchmarks](../benches/) for detailed metrics
- Read the [full documentation](https://FranekJemiolo.github.io/vectrill/)
        """
    )
    return


if __name__ == "__main__":
    app.run()
