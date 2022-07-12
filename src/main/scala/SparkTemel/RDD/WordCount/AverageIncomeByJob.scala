package SparkTemel.RDD.WordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageIncomeByJob {
  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Filter")

  val sc = new SparkContext(conf)

    val read_from_csv = sc.textFile("C:/Users/gulte/Desktop/Scala/income.csv")
      .filter(!_.contains("sirano"))

    def jobIncomes(line: String):(String,Double) = {
      val job = line.split(",")(3)
      val income = line.split(",")(5).toDouble
      (job, income)
    }

    val RDD_Income_Job = read_from_csv.map(jobIncomes)
    println("Read data from csv is : ")
    RDD_Income_Job.foreach(println)
    println("**************************************\n")

    val IncomeByJob = RDD_Income_Job.mapValues(x => (x,1))
    println("First Mapped Value : ")
    IncomeByJob.foreach(println)
    println("**************************************\n")

    val IncomeByJobRBK = IncomeByJob.reduceByKey((x,y)=>(x._1 + y._1, x._2 + y._2))
    println("Reduced By Key Data : ")
    IncomeByJobRBK.foreach(println)
    println("**************************************\n")

    val averageIncomeByJobMap = IncomeByJobRBK.mapValues( x => x._1 / x._2)
    println("Second Mapped Value : ")
    averageIncomeByJobMap.foreach(println)
    println("**************************************\n")


  }

}
