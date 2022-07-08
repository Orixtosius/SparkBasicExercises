package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Filter {
  def main(args: Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("Filter")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    println("\nCSV'den RDD oluşturmak\n")
    val rdd_from_csv = sc.textFile("OnlineRetail.csv")
    println(s"\nHEADER\n${rdd_from_csv.first()}\n")

    //Get rid of header data to avoid errors during casting
    val retailRDD = rdd_from_csv.mapPartitionsWithIndex(
      (idx,line) => if (idx == 0) line.drop(1) else line
    )

    //Verify previous operation
    println(s"\nThe first row of dataset is:\n${retailRDD.first()}\n")

    println("Quantity => 30\n")
    retailRDD.filter(x => x.split(";")(3).toInt > 30).take(5).foreach(println)

    println("Description => COFFEE and Unit Price => 20.0\n")
    retailRDD.filter(x => x.split(";")(5).trim.replace(",",".").toFloat > 20.0F)
      .filter(x => x.split(";")(2).contains("COFFEE")).foreach(println)

    //We use boolean expression that's why we need to return a boolean value
    def coffeePrice20 (line:String):Boolean = {
      var sonuc = true
      var Description:String = line.split(";")(2)
      var UnitPrice:Float = line.split(";")(5).trim.replace(",",".").toFloat

      sonuc = Description.contains("COFFEE") && UnitPrice > 20.0
      sonuc
    }
    println("Result with Function")
    retailRDD.filter(x => coffeePrice20(x)).take(5).foreach(println)
  }

}
