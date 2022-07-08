package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import shapeless.ops.nat.GT.>

import java.util

object ActionAndTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Test")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory","3g")
      .getOrCreate()

    val sc = spark.sparkContext
    println("Data creation has started\n")
    val welcomingText = sc.makeRDD(List("Merhaba Spark"))
    welcomingText.foreach(println)

    val numberList = sc.makeRDD(List(3,7,13,15,22,36,7,11,3,25))

    val infoText = sc.makeRDD(List("spark öğrenmek çok heyecan verici"))
    infoText.map(x => x.toUpperCase).foreach(println)

    val instructionText = sc.textFile("instruction.txt")
    val row = instructionText.count()
    println(s"\nLine of text named instruction is ${row}")
    /*
    val rowSpecialized = instructionText.countByValue()

    println(s"Count of each element is ${rowSpecialized}")
     */

    val words = instructionText.flatMap(x => x.split(" ")).count()
    println(s"Word count of text file named instruction is ${words}\n")

    val numberList2 = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    numberList.subtract(numberList2).foreach(x => print(s"-$x"))
    println()


    val newList = numberList.distinct()
    println(s"\nNew List is : ${newList.collect().mkString(",")}")

    val frequence = numberList.countByValue()
    println(s"\nWord count of by value from list is ${frequence}\n")

  }

}
