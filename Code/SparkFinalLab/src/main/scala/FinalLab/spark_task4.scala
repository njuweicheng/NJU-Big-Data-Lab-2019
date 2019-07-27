package FinalLab

import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}

object spark_task4 {

  class Character(var id: Int, var label: String, var rank: Double) extends Ordered[Character]{
    this.id = id
    this.label = label
    this.rank = rank

    override def compare(that: Character): Int = {
      val rankComparation = rank.compareTo(that.rank)
      if(rankComparation != 0)
        -rankComparation
      else
        label.compareTo(that.label)
    }

    override def toString: String = this.label + "\t" + this.rank.toString

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_task4").setMaster("local")
    val sc = new SparkContext(conf)

    val prRandomPara = 0.85
    val iterTimes = 15
    val resultPath = "task4_output"

    val graphDataRDD = sc.textFile("task3_output")
    var labelCount = 0
    val nameList = graphDataRDD.map(line => {
      labelCount = labelCount + 1
      (labelCount, line.toString.split('\t')(0))
    })

    val nameListCollection = nameList.collect()

    val nCharacters = nameListCollection.length

    var characters: Array[Character] = new Array[Character](nCharacters)

    val nameLabels = collection.mutable.Map[String, Int]()

    for(i <- nameListCollection.indices){
      characters(i) = new Character(i + 1, nameListCollection(i)._2, 0)
      nameLabels.put(nameListCollection(i)._2, nameListCollection(i)._1 - 1)
    }


    var ranks: Array[Double] = new Array[Double](nCharacters)

    for(i <- ranks.indices){
      ranks(i) = 1
    }


    def parseLine(host:String, suffix: String, currentRanks: Array[Double]): Array[Double] = {
      val res: Array[Double] = new Array[Double](nCharacters)
      val hostId = nameLabels.get(host)
      val pairs = suffix.replace("[","").replace("]","").split("\\|")
      for(pair <- pairs){
        val neighbor = pair.split(",")(0)
        val weight = pair.split(",")(1).toDouble
        res(nameLabels.getOrElse(neighbor, 0)) = currentRanks(nameLabels.getOrElse(host,0)) * weight * prRandomPara
      }
      res
    }

    def addVector(v1: Array[Double], v2:Array[Double]): Array[Double] = {
      for(i <- v1.indices){ v1(i) += v2(i) }
      v1
    }

    for(i <- 0 until iterTimes){
      var tmpRanks = graphDataRDD.map(line => parseLine(line.split('\t')(0), line.split('\t')(1), ranks))
        .reduce((a,b) => addVector(a,b))
      ranks = tmpRanks.map(ele => ele + (1 - prRandomPara))
    }

    for(i <- characters.indices){ characters(i).rank = ranks(i) }
    characters = characters.sorted

    val out = new FileWriter(resultPath, true)
    for(character <- characters){ out.write(character.toString + '\n')}
    out.close()

  }
}
