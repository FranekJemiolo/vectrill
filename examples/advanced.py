import marimo

__generated_with = "0.8.0"
app = marimo.App()


@app.cell
def __(mo):
    mo.md(
        r"""
        # Vectrill Advanced Features

        This notebook demonstrates Vectrill's advanced features including expression optimization, buffer pooling, and performance monitoring.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Expression Optimization

        Vectrill performs several optimizations on expressions to improve performance:
        - **Constant Folding**: Pre-computes constant expressions at planning time
        - **Common Subexpression Elimination (CSE)**: Avoids duplicate computation
        """
    )
    return


@app.cell
def __():
    # Example of constant folding
    # The expression (10 + 20) is pre-computed to 30
    # The expression (5 * 3) is pre-computed to 15
    
    # Before optimization:
    # result = (10 + 20) + (5 * 3)
    
    # After constant folding:
    # result = 30 + 15 = 45
    
    print("Constant folding example:")
    print("(10 + 20) + (5 * 3) = 45 (computed at planning time)")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Memory Optimization

        Vectrill uses buffer pooling to reduce allocation overhead. Arrow arrays are reused when possible instead of allocating new ones.
        """
    )
    return


@app.cell
def __():
    # Buffer pool statistics
    print("Buffer Pool Features:")
    print("- Reuses Arrow arrays to reduce allocations")
    print("- Pools arrays by data type")
    print("- Tracks pool statistics (total arrays, pools count)")
    print("- Global buffer pool for application-wide reuse")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Performance Counters

        Vectrill includes built-in performance counters for monitoring execution metrics:
        - Rows processed
        - Batch counts
        - Execution time
        - Memory allocations
        - Cache hits/misses
        """
    )
    return


@app.cell
def __():
    # Performance counter types
    counter_types = [
        "RowsProcessed",
        "BatchesProcessed",
        "TotalTimeUs",
        "Allocations",
        "MemoryBytes",
        "CacheHits",
        "CacheMisses"
    ]
    
    print("Available Performance Counters:")
    for counter_type in counter_types:
        print(f"  - {counter_type}")
    return counter_types,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Query Planning

        Vectrill's query planner performs several optimizations:
        - **Projection Elimination**: Removes unnecessary columns
        - **Predicate Pushdown**: Pushes filters closer to data sources
        - **Operator Fusion**: Combines compatible operators
        """
    )
    return


@app.cell
def __():
    # Query plan optimizations
    optimizations = [
        "Projection Elimination: Remove unused columns early",
        "Predicate Pushdown: Filter data before expensive operations",
        "Operator Fusion: Combine Map+Map into single operator",
        "Constant Propagation: Replace variables with constants"
    ]
    
    print("Query Plan Optimizations:")
    for opt in optimizations:
        print(f"  - {opt}")
    return optimizations,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Operator Fusion

        Vectrill automatically fuses compatible operators to reduce data passes through the pipeline.
        """
    )
    return


@app.cell
def __():
    # Example of operator fusion
    print("Operator Fusion Example:")
    print("")
    print("Before fusion:")
    print("  data.map(lambda x: x + 1).map(lambda x: x * 2)")
    print("  → 2 passes through data")
    print("")
    print("After fusion:")
    print("  data.map(lambda x: (x + 1) * 2)")
    print("  → 1 pass through data")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Performance Tips

        Here are some tips to maximize performance with Vectrill:
        """
    )
    return


@app.cell
def __():
    tips = [
        "Use appropriate batch sizes (1000-10000 rows per batch)",
        "Filter early to reduce data volume",
        "Use projection to only select needed columns",
        "Leverage window functions for time-based aggregations",
        "Monitor performance counters to identify bottlenecks",
        "Use buffer pooling for repeated allocations"
    ]
    
    print("Performance Tips:")
    for i, tip in enumerate(tips, 1):
        print(f"  {i}. {tip}")
    return tips,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Benchmarking

        Run performance benchmarks with:
        ```bash
        cargo bench --features performance
        ```
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Web UI

        Vectrill includes a web UI for real-time metrics and job inspection. Start it with:
        ```bash
        cargo run --features web-ui
        ```
        Then visit http://localhost:3000
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        - Check the [performance benchmarks](../benches/) for detailed metrics
- Read the [full documentation](https://FranekJemiolo.github.io/vectrill/)
- Explore the [source code](https://github.com/FranekJemiolo/vectrill)
        """
    )
    return


if __name__ == "__main__":
    app.run()
