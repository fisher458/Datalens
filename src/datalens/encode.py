import pathlib
import polars as pl



class Encoding:
    def __init__(self, filepath ,output_path ,**kwargs):
        self.filepath = pathlib.Path(filepath)
        self.output_path = pathlib.Path(output_path) if output_path else None
        self.lazy_df =  pl.scan_parquet(self.filepath)

        self.labelCol : list = kwargs.get("labelCol", [])
        self.oneHotCol : list = kwargs.get("oneHotCol", [])
        self.targetCol : dict = kwargs.get("targetCol", {})
        self.freqCol : list = kwargs.get("freqCol", [])
        self.cyclicalCol : dict = kwargs.get("cyclicalCol", {})



    def do_encoding(self) -> None:
        self.label_encode()


        self.save()

    def label_encode(self) -> None:
        
        self.lazy_df = self.lazy_df.with_columns([
            pl.col(column).cast(pl.Categorical).to_physical()
            
            for column in self.labelCol
        ])
        

    def one_hot_encode(self) -> None:
        pass

    def target_encode(self, target_column) -> None:
        pass

    def frequency_encode(self) -> None:
        pass

    def cyclical_encode(self, period) -> None:
        pass

    def save(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.lazy_df.sink_parquet(self.output_path, compression="zstd")
        except Exception as e:
            self.lazy_df.collect().write_parquet(self.output_path)
        

