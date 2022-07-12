package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Broadcast {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("DSV")

    val sc = new SparkContext(conf)

    def ReadFile(): Map[Int,String] = {
      val source = Source.fromFile("C:/Users/gulte/Desktop/Scala/products.csv")
      val lines = source.getLines().filter( x => !(x.contains("productId")) )
      var IDAndName: Map[Int,String] = Map()
      for (line <- lines){
        val id = line.split(",")(0).toInt
        val name = line.split(",")(2)
        IDAndName += (id->name)
      }
      return  IDAndName
    }

    val brodcastV = sc.broadcast(ReadFile)

    val read_orders = sc.textFile("C:/Users/gulte/Desktop/Scala/order_items.csv")
      .filter(!_.contains("orderItemName"))

    def OrderItemsRDD(line:String) = {
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemSubTotal = line.split(",")(4).toFloat
      (orderItemProductId, orderItemSubTotal)
    }
    val OrderItemsPairRDD = read_orders.map(OrderItemsRDD)
    val top10withIDs = OrderItemsPairRDD.reduceByKey((x,y)=>x+y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(10)
      .map(x => (x._2, x._1))

    val broadcastedItems = top10withIDs.map(x => (brodcastV.value(x._1), x._2))
    println("TOP 10 Most Sold Product")
    broadcastedItems.foreach(println)
  }

}
