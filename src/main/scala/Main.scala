import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DummySparkJob {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("DummySparkJob")
      .master("local[*]") // Runs locally using all cores
      .getOrCreate()

    import spark.implicits._

    // Sample data (could also read from a file)
    val dummyData = Seq(
      ("Alice", 25, "Engineer"),
      ("Bob", 30, "Data Scientist"),
      ("Charlie", 35, "Manager"),
      ("Diana", 28, "Analyst")
    )

    // Create a DataFrame
    val df: DataFrame = dummyData.toDF("Name", "Age", "Job")

    // Show the raw data
    println("=== Raw Data ===")
    df.show()

    // Transformation: Filter people older than 28
    val filteredDf = df.filter(col("Age") > 28)

    // Transformation: Add a new column
    val transformedDf = filteredDf.withColumn("Bonus", col("Age") * 10)

    println("=== Transformed Data ===")
    transformedDf.show()

    // Aggregation: Average age by job
    val avgAgeByJob = df.groupBy("Job")
      .agg(avg("Age").alias("AvgAge"))

    println("=== Average Age by Job ===")
    avgAgeByJob.show()

    // Write output (uncomment to use)
    // transformedDf.write.parquet("output/dummy_output.parquet")

    // Stop SparkSession
    spark.stop()
  }
}