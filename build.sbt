ThisBuild / scalaVersion := "2.13.12" // Spark 3.5.0 works best with Scala 2.13

lazy val root = project
  .in(file("."))
  .settings(
    name := "spark-scala-demo",
    version := "0.1.0-SNAPSHOT",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0", // Use %% for Scala 2.13
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.scalameta" %% "munit" % "1.0.0" % Test
    ),

    fork := true,
    javaOptions ++= Seq(
      "-Xms512M", "-Xmx1024M", "-XX:MaxPermSize=2048M", "-Dspark.master=local[*]"
    )
  )