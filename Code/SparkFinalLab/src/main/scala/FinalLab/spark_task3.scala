package FinalLab

import org.apache.spark.{SparkConf, SparkContext}

object spark_task3 {
  def mapper(item:String)={
    val itemInfo = item.trim().substring(1)
    val lineInfo = itemInfo.split("\t")
    val mapKeyInfo  = lineInfo(0).split(",")(0)
    (mapKeyInfo, lineInfo(1).toInt)
  }

  def reducer(item:String, keysIndex:collection.Map[String, Int]) ={
    val itemInfo = item.trim().substring(1)
    val lineInfo = itemInfo.split("\t")
    val mapKeyInfo  = lineInfo(0).split(",")(0)
    var mapValueInfo = lineInfo(0).split(",")(1)
    mapValueInfo = mapValueInfo.substring(0, mapValueInfo.length-1)
    mapValueInfo = mapValueInfo+","+(lineInfo(1).toFloat/keysIndex(mapKeyInfo)).formatted("%.4f")
    (mapKeyInfo, mapValueInfo)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task3").setMaster("local")
    val sc = new SparkContext(conf)

    val inputFile = "task2_output"
    val outputFile = "task3_output"

    val rdd = sc.textFile(inputFile)

    // 求取每个人与其他人物的共现总次数，存为map
    val keysIndex = rdd.map(line => mapper(line))
      .reduceByKey(_ + _).collectAsMap()

    // 求取每个人的共现人物的比例
    val results = rdd.map(line => reducer(line, keysIndex))
      .reduceByKey(_ + "|" + _).map(line => line._1+"\t["+line._2+"]")

    // results.foreach(println)
    results.saveAsTextFile(outputFile)
  }
}
