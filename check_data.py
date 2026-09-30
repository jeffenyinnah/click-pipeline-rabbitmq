from pathlib import Path
import pyarrow.dataset as ds


path = Path(__file__).parent / "data" / "raw"
table = ds.dataset(path, format="parquet", partitioning="hive").to_table()
print(table.num_rows)
print(table.slice(0, 3).to_pylist())