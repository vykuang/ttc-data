import marimo

__generated_with = "0.22.4"
app = marimo.App()


@app.cell
def _(mo):
    mo.md(r"""
    # Complete GTFS - Dimension tables

    Inspecting the dimensions like stops, trips, stop-times, and routes, and converting from txt to parquet
    """)
    return


@app.cell
def _():
    from pathlib import Path
    import polars as pl
    from io import StringIO


    return Path, StringIO, pl


@app.cell
def _(Path):
    dims_dir = Path('../data/dims')
    return (dims_dir,)


@app.cell
def _(dims_dir):
    def read_dims(dim_name: str = 'routes', top_n: int = 10):
        with open(dims_dir / f'{dim_name}.txt', 'r') as rf:
            for i, line in enumerate(rf):
                if i >= top_n:
                    break
                yield line

    return (read_dims,)


@app.cell
def _(read_dims):
    routes = list(read_dims())
    routes
    return (routes,)


@app.cell
def _(routes):
    routes_io = ''.join(routes)
    print(routes_io)
    return (routes_io,)


@app.cell
def _(routes_io):
    routes_io
    return


@app.cell
def _(StringIO, pl, routes_io):
    routes_df = pl.read_csv(StringIO(routes_io))
    routes_df
    return


@app.cell
def _(read_dims):
    stops_sample = list(read_dims('stops', 5))
    stops_sample
    return (stops_sample,)


@app.cell
def _(pl, stops_sample):
    stops_df = pl.read_csv(''.join(stops_sample).encode())
    stops_df
    return


@app.cell
def _(dims_dir, pl):
    # idiomatic way to stream csv to parquet
    (
        pl.scan_csv(dims_dir / 'routes.txt')
        .sink_parquet(dims_dir / 'routes.parquet')
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
