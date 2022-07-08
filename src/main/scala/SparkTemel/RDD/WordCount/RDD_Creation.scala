package SparkTemel.RDD.WordCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object RDD_Creation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
/*
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("RDD_Creation")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    //RDD API'si spark Context ile kullanılabiliyor.
    val sc = spark.sparkContext
*/
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("RDD_Creation")
      .setExecutorEnv("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    println("Listeden RDD oluşturmak")
    val rddFromList = sc.makeRDD(List(1,2,3,4,5,6,7,9))
    rddFromList.take(3).foreach(println)

    println("\nCSV'den RDD oluşturmak")
    val rdd_from_text = sc.textFile("OnlineRetail.csv")
    rdd_from_text.take(3).foreach(println)
  }
}
