import pathlib
import polars as pl



class toParquet:
    
    def __init__(self,filepath, output_path=None):
        self.filepath = pathlib.Path(filepath)
        self.output_path = pathlib.Path(output_path) if output_path else None

        self.lazy_df =  pl.scan_csv(self.filepath)



    def convert_to_parquet(self):
        self.lazy_df.sink_parquet(self.output_path, compression="zstd")

    
        



