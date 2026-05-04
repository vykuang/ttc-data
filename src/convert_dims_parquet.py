import polars as pl
from pathlib import Path
import argparse
from constants import DIM_SCHEMA_MAP

def convert_parquet(dim_type: str, dims_dir: Path):
    schema = DIM_SCHEMA_MAP.get(dim_type)
    if not schema:
        raise ValueError(f"{dim_type} not supported")
    dim_path = dims_dir / f'{dim_type}.txt'
    pq_path = dims_dir / f'{dim_type}.parquet'
    if not dim_path.exists():
        raise FileNotFoundError(f"{dim_path.absolute()} not found")
    (
        pl.scan_csv(
            dim_path, 
            schema_overrides=schema,
        )
        .sink_parquet(pq_path)
    )
    return pq_path

def convert_dims(dims_dir: Path = Path('../data/dims')):
    # glob dims
    #dim_glob = Path(dims_dir).glob('*.txt')
    for dim in DIM_SCHEMA_MAP:
        pq_path = convert_parquet(dim, dims_dir)
        print(f'{dim} saved as {pq_path}')
        
def main(dims_dir):
    convert_dims(dims_dir)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dims_dir', '-d', type=Path, default='data/dims')
    args = parser.parse_args()
    main(args.dims_dir)