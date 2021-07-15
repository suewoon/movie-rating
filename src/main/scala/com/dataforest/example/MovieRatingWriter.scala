package com.dataforest.example

import java.sql.Timestamp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}

object MovieRatingWriter {
  // Create a schema from case class
  case class Movie(movieId: Int,title: String, genres: String)
  case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Timestamp)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieRatingWriter")
    //  .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Set loglevel
    spark.sparkContext.setLogLevel("ERROR")

    val movie_schema = ScalaReflection.schemaFor[Movie].dataType.asInstanceOf[StructType]
    val rating_schema =  ScalaReflection.schemaFor[Rating].dataType.asInstanceOf[StructType]

    // movie.csv to dataframe
    val movie_df_temp = spark.read
      .option("header", "true")
      .schema(movie_schema)
      .csv(s"${args(0)}/movie.csv")
    val movie_df = movie_df_temp
      .select($"movieId", $"title", explode(split($"genres", "\\|")).as("genre"))  // Explode the column genre as it contains multiple values
      .withColumn("releasedYear", regexp_extract($"title","\\((\\d{4})\\)", 1))  // Extract released date (year) from title column
      .withColumn("releasedYear", $"releasedYear".cast(IntegerType)) // Cast string type to integer

    // rating.csv to dataframe
    val rating_df_temp = spark.read
      .option("header", "true")
      .schema(rating_schema)
      .csv(s"${args(0)}/rating.csv")
    val rating_df_filtered = rating_df_temp.groupBy($"movieId")
      .agg(count("*").as("count")).filter($"count" > 10) // Filter out records where only few people rate the movie.
    val rating_df = rating_df_temp.join(rating_df_filtered, rating_df_temp("movieId") === rating_df_filtered("movieId"))
      .select($"userId", rating_df_temp("movieId"), $"rating", year($"timestamp").as("year"), month($"timestamp").as("month")) // We will use year, month as a partiion key later

    // join two dataframes
    val movie_rating_df = movie_df.join(rating_df, Seq("movieId"), "inner")

    movie_rating_df.repartition($"year", $"month")
      .write.partitionBy("year", "month")
      .mode(SaveMode.Append)
      .parquet(s"${args(0)}/movie_rating")

  }
}
