import marimo

__generated_with = "0.8.0"
app = marimo.App()


@app.cell
def __(mo):
    mo.md(
        r"""
        # Vectrill Getting Started

        Welcome to Vectrill! This notebook will introduce you to the basic concepts and API of the Vectrill streaming engine.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## What is Vectrill?

        Vectrill is a high-performance Arrow-native streaming engine with Python DSL and Rust execution core. It combines:
        - Spark-like API and query planning
        - Flink-like streaming semantics
        - Apache Arrow's zero-copy columnar memory
        - Rust's performance and memory safety
        - Python's ergonomics and ecosystem
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Installation

        Install Vectrill from source:

        ```bash
        git clone https://github.com/FranekJemiolo/vectrill.git
        cd vectrill
        cargo build --release
        pip install maturin
        maturin develop
        ```
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Basic Usage

        Vectrill provides a Python DSL for building streaming pipelines. Here's a simple example:
        """
    )
    return


@app.cell
def __():
    import polars as pl

    # Create sample data
    data = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value": [100, 200, 300, 400, 500],
        "active": [True, False, True, False, True]
    })
    return data,


@app.cell
def __(data):
    print(data)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Core Concepts

        Vectrill is built around several core concepts:

        1. **Sequencer**: Orders events by timestamp and handles out-of-order data
        2. **Micro-batching**: Groups events into batches for efficient processing
        3. **Operators**: Transform and aggregate data (Map, Filter, Aggregate)
        4. **Windows**: Group data by time windows (Tumbling, Sliding, Session)
        5. **Watermarks**: Track event time progress and handle late data
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ## Next Steps

        - Check out the [streaming notebook](streaming.py) for more advanced examples
- Explore the [advanced notebook](advanced.py) for optimization techniques
- Read the [full documentation](https://FranekJemiolo.github.io/vectrill/)
        """
    )
    return


if __name__ == "__main__":
    app.run()
