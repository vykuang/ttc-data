import polars as pl
from application.ports import TabularFormat

class ParquetPolarsFormat(TabularFormat[pl.DataFrame]):
    def read(self, path) -> pl.DataFrame:
        return pl.read_parquet(path)
    
    def write(self, df: pl.DataFrame, path) -> None:
        df.write_parquet(path)
        