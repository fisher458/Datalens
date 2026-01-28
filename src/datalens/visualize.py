import pathlib
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns



class Visualizer:
    
    def __init__(self, filepath, output_path=None, **kwargs):
        self.filepath = pathlib.Path(filepath)
        self.output_path = pathlib.Path(output_path) if output_path else None

        self.lazy_df =  pl.scan_parquet(self.filepath)

        self.bar_chartCol : list = kwargs.get("bar_chartCol", [])
        self.histogramCol : list = kwargs.get("histogramCol", [])


    def visualize(self):
        if self.bar_chartCol:
            self.bar_chart()
        if self.histogramCol:
            self.histogram()



    

    def histogram(self):


        pass

    
    
    def bar_chart(self):
        if not self.bar_chartCol:
            return
        

        print(f"Starting bar chart generation for columns: {self.bar_chartCol}")


        for col in self.bar_chartCol:


            try:
                 

                pdf,xCol, yCol = self._getColumnCount(col)

                self._plot_single_bar(pdf, xCol, yCol)

                self._output_handler( f"{col}_bar_chart.pdf")

                print(f"Successfully generated chart for: {col}")
            
            except Exception as e:
                 
                 print(f"Error processing column '{col}': {e}")




    def _getColumnCount(self,columnName):

        df_pl = (
            self.lazy_df
            .select(
                pl.col(columnName).value_counts(sort=True)
            )
            .collect()
            .unnest(columnName)
            
        )

        pdf = df_pl.to_pandas()
         
        xCol = columnName

        y_candidates = [c for c in pdf.columns if c != xCol]
        
        if not y_candidates:
            raise ValueError(f"Cannot identify count column for {columnName}")
        
        yCol = y_candidates[0]
        
        return pdf, xCol, yCol



    def _plot_single_bar(self, pdf, x_col, y_col):
        
        plt.figure(figsize=(10, 6))
        
        sns.barplot(
            data=pdf, 
            x=x_col, 
            y=y_col, 
            palette="viridis", 
            hue=x_col, 
            legend=False
        )
        
        sns.despine()
        plt.title(f"Distribution of {x_col}")
        plt.xlabel(x_col)
        plt.ylabel("Count")
        plt.xticks(rotation=45)



    def _output_handler(self,filename):
        plt.tight_layout()

        if self.output_path:
                
                self.output_path.mkdir(parents=True, exist_ok=True)
                output_file = pathlib.Path(self.output_path / filename)
                
                plt.savefig(output_file, dpi=300)
                plt.close()
        else:
                plt.show()
                plt.close()

            




            



        

    