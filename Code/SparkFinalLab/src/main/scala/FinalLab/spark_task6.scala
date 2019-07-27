package FinalLab

import org.apache.spark.{SparkContext, SparkConf}

object spark_task6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task6").setMaster("local")
    val sc = new SparkContext(conf)

    val inputDir = "task5_output"
    val outDir = "task6_output"

    val rdd = sc.textFile(inputDir)

    val results = rdd.map(line => (line.split("\t")(1), line.split("\t")(0)))
      .reduceByKey(_ + "|" + _).map(line => line._1+"\t"+line._2)

    // results.foreach(println)
    results.saveAsTextFile(outDir)
  }
}
