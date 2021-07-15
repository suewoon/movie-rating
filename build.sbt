name := "movie-rating"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.lz4" % "lz4-java" % "1.4.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided, // As it is already present in the Spark distribution.
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

// Avoids SI-3623
target := file("/tmp/sbt/movie-rating")

assemblyOption in assembly := (assemblyOption in
  assembly).value.copy(includeScala = false) // exclude all scala runtime JARs
test in assembly := {} // skip the tests when running the assembly task
mainClass in assembly := Some("com.dataforest.example.MovieRatingWriter") // declare main task