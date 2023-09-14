from pyspark.sql import SparkSession

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Spark Temporary View from CSV Demo") \
        .master("local[*]") \
        .getOrCreate()

    # Load data from CSV into DataFrame
    df = spark.read.csv("data.csv", header=True, inferSchema=True)

    # Create or replace a temporary view
    df.createGlobalTempView("NYC_Taxi_Data")

    # Query the temporary view using Spark SQL
    result = spark.sql("SELECT * FROM NYC_Taxi_Data")
    result.show()

    # Block the script from exiting
    try:
        print("SparkSession is alive. Press Ctrl+C to terminate.")
        while True:
            pass
    except KeyboardInterrupt:
        print("Stopping SparkSession...")
        spark.stop()

if __name__ == "__main__":
    main()
