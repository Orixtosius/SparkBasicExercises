package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("RDD")

    val sc = new SparkContext(conf)
    val read_orders = sc.textFile("C:/Users/gulte/Desktop/Scala/order_items.csv")
      .filter(!_.contains("orderItemName"))
    val read_products = sc.textFile("C:/Users/gulte/Desktop/Scala/products.csv")
      .filter(!_.contains("productId"))
    println("Example of Order Item DataSet\n")
    read_orders.take(5).foreach(println)
    println("\nExample of Product DataSet\n")
    read_products.take(5).foreach(println)
    println("****************************************************************")

    def MakeOrdersPairRDD(line:String) = {
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(5)
      (orderItemProductId,(orderItemName,orderItemOrderId,orderItemProductId,
        orderItemQuantity,orderItemSubTotal,orderItemProductPrice))
    }

    val OrderItemsPairRDD = read_orders.map(MakeOrdersPairRDD)
    println("\nExample of OrderItemsPair RDD\n")
    OrderItemsPairRDD.take(5)foreach(println)
    println("****************************************************************")

    def MakeProductPairRDD(line:String) = {
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)
      (productId,(productId,productCategoryId,productName,productDescription,productPrice,productImage))
    }

    val ProductItemsPairRDD = read_products.map(MakeProductPairRDD)
    println("\nExample of ProductItemsPair RDD\n")
    ProductItemsPairRDD.take(5)foreach(println)
    println("****************************************************************")

    val joinedRDD = OrderItemsPairRDD.join(ProductItemsPairRDD)
    println("\nExample of Joined RDD\n")
    joinedRDD.take(5).foreach(println)
    println("****************************************************************")

    println("\nCROSS CHECK")
    println(s"Row in Order RDD is ${OrderItemsPairRDD.count()}")
    println(s"Row in Product RDD is ${ProductItemsPairRDD.count()}")
    println(s"Row in Joined RDD is ${joinedRDD.count()}")
    println("****************************************************************")
  }
}
