import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.nio.file.{Files, Paths, StandardOpenOption}

object NetflixEDA {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Netflix EDA")
      .master("local[*]")
      .getOrCreate()

    val netflixData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./netflix_titles.csv")

    val outputDir = "./output"
    Files.createDirectories(Paths.get(outputDir))

    writeToFile(s"${outputDir}/schema.txt", netflixData.schema.treeString)

    saveDataFrame(netflixData.limit(10), s"${outputDir}/sample_data")

    val typeCount = netflixData.groupBy("type").count()
    saveDataFrame(typeCount, s"${outputDir}/type_count")

    val topCountries = netflixData.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    saveDataFrame(topCountries, s"${outputDir}/top_countries")

    val popularGenres = netflixData.groupBy("listed_in")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    saveDataFrame(popularGenres, s"${outputDir}/popular_genres")

    val titlesPerYear = netflixData.filter(col("release_year").isNotNull)
      .groupBy("release_year")
      .count()
      .orderBy("release_year")
    saveDataFrame(titlesPerYear, s"${outputDir}/titles_per_year")

    val nullCounts = netflixData.columns.map { colName =>
      val nullCount = netflixData.filter(col(colName).isNull || col(colName) === "").count()
      s"$colName: $nullCount"
    }
    writeToFile(s"${outputDir}/null_counts.txt", nullCounts.mkString("\n"))

    spark.stop()
  }

 
  def saveDataFrame(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }


  def writeToFile(path: String, content: String): Unit = {
    Files.write(Paths.get(path), content.getBytes, StandardOpenOption.CREATE)
  }
}
