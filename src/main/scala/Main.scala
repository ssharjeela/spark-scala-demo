import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
//import java.awt.Window

case class Employee(Name: String, Age: Int, Job: String, Salary: java.lang.Integer, JoinDate: String, DeptId: Int)
case class Department(DeptId: Int, DeptName: String)

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
      Employee("Alice", 25, "Engineer", 60000, "2020-01-15", 101),
      Employee("Bob", 30, "Data Scientist", 75000, "2019-06-10", 102),
      Employee("Charlie", 35, "Manager", 90000, "2015-03-12", 101),
      Employee("Diana", 28, "Analyst", 58000, "2021-07-01", 103),
      Employee("Eve", 45, "Manager", 95000, "2012-11-20", 101),
      Employee("Frank", 23, "Intern", 30000, "2023-01-10", 104),
      Employee("Grace", 29, "Engineer", 64000, "2018-05-15", 101),
      Employee("Heidi", 32, "Data Scientist", null, "2020-09-30", 102)
    )

    // Create a DataFrame
    val df: DataFrame = dummyData.toDF("Name", "Age", "Job", "Salary", "JoinDate", "DeptId")


    val departments = Seq(
      (101, "Engineering"),
      (102, "Data"),
      (103, "Finance"),
      (104, "HR")
    ).toDF("DeptId","DeptName")

    // Show the raw data
    println("=== Raw Data ===")
    df.show()
    departments.show()

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

    val olderAge = df.filter(col("Age") > 28)
    println ("==+ older age > 25")
    olderAge.show()

    val joinWithDpt = df.join(departments, "DeptId")
    joinWithDpt.show()

    val avgSalary = df.agg(avg("Salary")).first().getDouble(0)
    
    val salaryFixed = df.na.fill(Map("Salary" -> avgSalary)) 
    salaryFixed.show()
    
    joinWithDpt.select("Name","Age","DeptName","Job").show()

    val win = Window.partitionBy("DeptId").orderBy($"Salary".desc)
    val ranked = joinWithDpt.withColumn("Rank", rank().over(win))

    println("=== Salary Rank Per Department ===")
    ranked.select("Name","DeptName", "Salary", "Rank").show()
    







    // Write output (uncomment to use)
    // transformedDf.write.parquet("output/dummy_output.parquet")

    // Stop SparkSession
    spark.stop()
  }
}