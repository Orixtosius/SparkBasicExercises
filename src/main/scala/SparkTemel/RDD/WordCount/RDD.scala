package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object RDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD_deneme_v1")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext
    println("Please type down the location of text file.")
    val path = scala.io.StdIn.readLine()
    println("----------------------------------------------------------")
    val storyRDD = sc.textFile(path)
    println(storyRDD.count())
    val words = storyRDD.flatMap(line => line.split(" "))
    val wordCount = words.map(word => (word,1)).reduceByKey((x,y) => x+y)
    println(wordCount.count())
    println("----------------------------------------------------------")
    words.take(10).foreach(println)
    println("----------------------------------------------------------")
  }
}
