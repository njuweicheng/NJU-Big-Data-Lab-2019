package FinalLab

import java.io.File

import org.ansj.library.DicLibrary
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.nlpcn.commons.lang.tire.domain.Forest

import scala.collection.mutable.ArrayBuffer

object spark_task1 {
  def getFiles(path:String):Array[File] = {
    val subPath = new File(path).listFiles()
    val files = subPath.filter(!_.isDirectory)
    val dirs = subPath.filter(_.isDirectory)
    files ++ dirs.flatMap(dir => getFiles(dir.getPath))
  }

  def getNamesInLine(line: String, dictKey: String, strNature: String): String = {
    val res = ArrayBuffer[String]()
    val terms = DicAnalysis.parse(line, DicLibrary.get(dictKey))

    terms.forEach(term =>
      if (term.getNatureStr.equals(strNature)){
        res += term.getName
      }
    )
    res.toArray.mkString(" ")
  }


  def main(args: Array[String]): Unit = {
    //BasicConfigurator.configure()

    val conf = new SparkConf().setAppName("FinalLab_task1").setMaster("local")
    val sparkContext = new SparkContext(conf)

    val nameListPath = "people_name_list.txt"
    val inputPath = "novels"

    val dataFiles = getFiles(inputPath)
    //files.foreach(println)

    val dictKey = "MyDict"
    val strNature = "name"
    DicLibrary.put(dictKey, dictKey, new Forest())
    val nameList = sparkContext.textFile(nameListPath)
    nameList.foreach(name => DicLibrary.insert(dictKey, name.toString.trim, strNature, 1000))
    //nameList.foreach(name => DicLibrary.insert(DicLibrary.DEFAULT, name.toString.trim, "name", 1000))

    for(dataFile <- dataFiles){
      val data = sparkContext.textFile(dataFile.getPath)
      val filePath = "task1_output/" + dataFile.getName.split("\\.")(0)
      // map, filter and save
      data.map(line => getNamesInLine(line, dictKey, strNature)).filter(line => line.length > 0).saveAsTextFile(filePath)

      //val findNames = data.map(line => getNamesInLine(line, dictKey, strNature)).filter(line => line.length > 0).collect()

      //val findNamesRDD = sparkContext.parallelize(findNames)

      //findNamesRDD.saveAsTextFile(filePath)

    }

  }
}
