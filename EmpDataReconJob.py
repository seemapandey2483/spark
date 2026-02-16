from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as _sum

class RealTimeDeltaETLPipeline:

    def start(self):

        # Create Spark Session
        spark = SparkSession.builder \
            .appName("Batch Delta ETL") \
            .getOrCreate()

        input_path = "/Volumes/workspace/default/test/emp.csv"
        delta_path = "/Volumes/workspace/default/test/employee_delta"

        # -------------------------
        # Read CSV (Batch)
        # -------------------------
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(input_path)

        # -------------------------
        # Source Reconciliation
        # -------------------------
        source_count = df.count()
        source_salary_total = df.select(_sum("Salary")).collect()[0][0]

        print("===== SOURCE RECON =====")
        print(f"Source Count: {source_count}")
        print(f"Source Salary Total: {source_salary_total}")

        # -------------------------
        # Clean Data
        # -------------------------
        df_clean = df.dropna() \
                     .dropDuplicates(["EmpID"]) \
                     .filter(col("Salary") > 60000)

        rejected_count = source_count - df_clean.count()

        # -------------------------
        # Transform
        # -------------------------
        df_transformed = df_clean.withColumn(
            "Tax",
            round(col("Salary") * 0.1, 2)
        )

        target_count = df_transformed.count()
        target_salary_total = df_transformed.select(_sum("Salary")).collect()[0][0]
        target_tax_total = df_transformed.select(_sum("Tax")).collect()[0][0]

        print("===== TARGET RECON =====")
        print(f"Target Count: {target_count}")
        print(f"Rejected Count: {rejected_count}")
        print(f"Target Salary Total: {target_salary_total}")
        print(f"Target Tax Total: {target_tax_total}")

        # -------------------------
        # Write to Delta
        # -------------------------
        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_path)

        print("===== LOAD COMPLETE =====")

        spark.stop()


if __name__ == "__main__":
    app = RealTimeDeltaETLPipeline()
    app.start()
