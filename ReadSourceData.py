from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

class RealTimeDeltaETLPipeline:

    def start(self):

        # Create Spark Session
        spark = SparkSession.builder \
            .appName("Real-Time Delta ETL") \
            .getOrCreate()

        # Input folder for CSVs
        input_path = "/Volumes/workspace/default/test/emp.csv"  

        # Read CSV as stream
        df_stream = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(input_path)

        # Clean data
        df_clean = df_stream.dropna() \
                            .dropDuplicates(["EmpID"]) \
                            .filter(col("Salary") > 60000)

        # Transform data (Tax calculation)
        df_transformed = df_clean.withColumn("Tax", round(col("Salary") * 0.1, 2))

        # Write to Delta Table (append mode)
        delta_path = "/Volumes/workspace/default/test/employee_delta"
    

if __name__ == "__main__":
    app = RealTimeDeltaETLPipeline()
    app.start()