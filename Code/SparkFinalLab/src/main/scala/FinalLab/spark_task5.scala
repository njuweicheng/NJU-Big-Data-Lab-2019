package FinalLab

import org.apache.spark.{SparkConf, SparkContext}

object spark_task5 {
  def getNewLabel(line:String,MapOfLabel:collection.mutable.Map[String,String]): String = {
    //返回一个String
    var line_ = line.replace("[", "")
    var Neighbours = line_.replace("]", "").split("\\|")
    var FinalLabel = collection.mutable.Map[String, Float]() //标签加float类型
    for (name <- Neighbours) {
      var temp = name.split(",")
      //寻找标签并加上权重
      var label = MapOfLabel(temp(0))
      //得到便签
      var value = temp(1).toFloat //得到权重
      if (FinalLabel.contains(label)) {
        var newValue = value + FinalLabel(label)
        FinalLabel.put(label, newValue)
      }
      else {
        FinalLabel.put(label, value)
      }
    }
    //寻找最大的标签并返回
    FinalLabel.maxBy(_._2)._1
  }

  def main(args: Array[String]): Unit = {
    //首先读取文件内容，具体内容为实验三的输出
    val outputDir = "task5_output"
    val conf = new SparkConf().setAppName("LPA").setMaster("local")
    val sc = new SparkContext(conf)
    val task3_out = sc.textFile("task3_output").filter(x => x.length > 0) //读取文件内容

    var MapOfLabel = collection.mutable.Map[String,String]()
    //首先是初始化，将每个标签设置为自己的标签
    val init = task3_out.map(x => (x.split("\t")(0),x.split("\t")(0))).collect()
    init.foreach(a => MapOfLabel.put(a._1,a._2))
    //进行标签传播算法
    for(x <- 0 until 15){ //由于便签震荡，只传播15次
      println("第"+x+"轮开始运行")
      val Graph = sc.parallelize(sc.textFile("task3_output").map(x => x.split("\t")).collect()) //读取文件内容
      println("开始标签传播")
      var res = Graph.map(line => (line(0),getNewLabel(line(1),MapOfLabel))).collect()
      res.foreach(a => MapOfLabel.put(a._1,a._2))
    }

    sc.parallelize(MapOfLabel.toSeq).sortBy(_._2).map(line => {
      val word = line._1
      val label = line._2
      word + "\t" + label
    }).saveAsTextFile(outputDir)
  }
}
