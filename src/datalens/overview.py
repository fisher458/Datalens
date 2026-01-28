import polars as pl
import pathlib



class Overview:
    def __init__(self, filepath,output_path=None):
        self.filepath = pathlib.Path(filepath)
        self.output_path = pathlib.Path(output_path) if output_path else None

        self.lazy_df =  pl.scan_parquet(self.filepath)


    def do_overview(self):
        

        col_names, shape = self.show_shape()

        self.write_into_file(f"Shape: {shape}\n", mode="w")

        self.write_into_file(f"Columns Names: {col_names}\n")

        self.write_into_file("First 10 rows:\n")

        with pl.Config(tbl_cols=-1, tbl_rows=-1, fmt_str_lengths=100):
            df = self.lazy_df.collect()
            describe = self.get_description(df)
            head = self.get_head(df, n=10)
            self.write_into_file(head)
            self.write_into_file("\nDescription:\n")
            self.write_into_file(describe)
        




        

        

       



        

           


    def write_into_file(self, content, mode="a"):
        if self.output_path:
            with open(self.output_path, mode,encoding="utf-8") as f:
                f.write(str(content) + "\n")


  


    
        
    
    def show_shape(self):
        
        schema = self.lazy_df.collect_schema()
        col_names = schema.names()
        columns_count = len(col_names)
        rows_count = self.lazy_df.select(pl.len()).collect().item()
        shape = (rows_count, columns_count)


        return   col_names, shape
    
    



    def get_col(self,df, column):
        return self.lazy_df.select(column).collect()
    



    def get_head(self,df, n=10):
        return df.head(n)
    

    def get_description(self, df):
        return df.describe()
    
    
    
    


    




    




