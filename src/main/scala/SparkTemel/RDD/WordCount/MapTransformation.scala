package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MapTransformation {

  def main(args: Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val config = new SparkConf().setMaster("local[4]").setAppName("MapTrans")
    val sc = new SparkContext(config)

    val retailRDD = sc.textFile("OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))
    println(s"\n...HEADER...\n${retailRDD.first()}\n")

    /*
    //Get rid of header data to avoid errors during casting
    val retailRDD = rdd_from_csv.mapPartitionsWithIndex(
      (idx,line) => if (idx == 0) line.drop(1) else line
    )

     */

    def filterByInvoiceCode(line: String) = {
      var result = true
      result = line.split(";")(0).startsWith("C")
      result
    }
/*
    val totalAmount = retailRDD.reduce(x=>{
      var isCanceled = if (x.split(";")(0).startsWith("C")) 1; else 0
      var RevenuePerProduct = x.split(";")(3).trim.replace(",",".").
        toFloat * x.split(";")(5).trim.replace(",",".").toFloat
      var result = 0.0
      result += isCanceled*RevenuePerProduct
      result.toString
    })

 */


  val totalAmount = retailRDD.map(x => {
    var isCanceled = if (x.split(";")(0).startsWith("C")) true else false
    var RevenuePerProduct = x.split(";")(3).trim.replace(",",".").
    toDouble * x.split(";")(5).trim.replace(",",".").toDouble
    (isCanceled, RevenuePerProduct)
})


    totalAmount.take(5).foreach(println)

    println(s"\n......Lost Revenue to Canceled Operations......\n")
    totalAmount.map(x => (x._1, x._2)).reduceByKey((a,b) => a+b)
      .filter(t => t._1).map(z => z._2).foreach(println)

     /*
case class ProductRefundStatus(isCanceled:Boolean, productValue:Double)
    val totalAmount = retailRDD.map(x => {
      var isCanceled:Boolean = if (x.split(";")(0).startsWith("C")) true else false
      var quantity:Double = x.split(";")(3).toDouble
      var price:Double = x.split(";")(5).trim.replace(",",".").toDouble
      ProductRefundStatus(isCanceled, quantity*price)
    })


    totalAmount.take(5).foreach(println)

    println(s"\n......Lost Revenue to Canceled Operations......\n")
    totalAmount.map(x => (x.isCanceled, x.productValue))
      .reduceByKey((a,b) => (a+b))
      .filter(t => t._1 == true)
      .map(z => z._2)
      .foreach(println)

     */





  }

}
