package FinalLab

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object spark_task2 {
  def getNamesInLine(line:String): Array[String] = { //输入为String 输出为两两之间的代码
    //首先定义集合进行去重.
    val strs = line.split(" ") //按空格划分
  val myset = mutable.Set[String]()
    strs.foreach(x => myset.add(x)) //加入集合
    val nameinline = myset.toArray
    //对于每一行都进行操作
    val uniquePairs = for {
      (x, idxX) <- nameinline.zipWithIndex
      (y, idxY) <- nameinline.zipWithIndex
      if idxX != idxY
    } yield ("<"+x+","+y+">")
    //输出为两个键值对
    return uniquePairs
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupSort").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("task1_output/*")
    val resArray = lines.flatMap(x => getNamesInLine(x)).filter(x => x.length > 0).collect()
    val res = sc.parallelize(resArray).map((_,1)).reduceByKey(_+_).sortByKey().map(line => {
      val word = line._1
      val cnt = line._2
      word + "\t" + cnt
    }) //结果文件
    res.saveAsTextFile("task2_output")
    sc.stop()
  }
}
