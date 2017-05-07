import java.util

import scala.collection.mutable
import scala.io.Source

/**
  * Represents a set in a cache, which implements the LRU evicting policy
  * @param setSize The size of the set (i.e 4)
  */
class CacheSet( val setSize : Int ) {

  var lines = new mutable.ListBuffer[Int]()

  /**
    * Reads the line with the given id and inserts it to the cache if necessary
    * @param tag The tag of the line
    * @return True if the line was in the set
    */
  def read( tag : Int ) : Boolean = {
    val prevNumLines = lines.length
    lines.diff(Seq[Int](tag))
    val postNumLines = lines.length
    lines = tag +=: lines
    if(lines.length >= setSize) {
      lines = lines.dropRight(1)
    }
    return prevNumLines != postNumLines
  }
}


class Cache( val cacheSizeB : Int = 32*1024,
                      val cacheLineB : Int = 64,
                      val setAssociativity : Int = 4,
                      val elementSizeB : Int = 8) {

  val numLines = cacheSizeB / cacheLineB
  val numSets = numLines / setAssociativity
  val sets = Array.fill[CacheSet](numSets){new CacheSet(setAssociativity)}
  val elementsPerLine = cacheLineB / elementSizeB
  val statistics = new mutable.HashMap[Int,Int]()

  def readElement( element : Int ) = {
    val tag = (element / elementsPerLine)
    val index = (tag % numLines)
    val setId = index % numSets
    if(!sets(setId).read(tag)) {
     statistics.put(tag,statistics.getOrElse(tag,0)+1)
    }
  }
}

object GraphCacheSimulator extends App {

  def printUsageMessage(): Unit = {
    println("--input-file <path> The path to the input graph file")
    println("--cache-size <num> The size of the cache in bytes (default=32768) ")
    println("--line-size <num> The size of the cache line in bytes (default=64)")
    println("--set-associativity <num> The set associativity (default=4)")
    println("--element-size <num> The size of each element (default=8)")
  }

  def nextOption( args : List[String]): Unit = {
    args match {
      case "--input-file" :: value :: tail => inputFile = value
      case "--cache-size" :: value :: tail => cacheSizeB = value.toInt
      case "--line-size" :: value :: tail => cacheLineB = value.toInt
      case "--set-associativity" :: value :: tail => setAssociativity = value.toInt
      case "--element-size" :: value :: tail => elementSize = value.toInt
      case Nil => Unit
    }
  }

  var inputFile = ""
  var cacheSizeB = 32*1024
  var cacheLineB = 64
  var setAssociativity = 4
  var elementSize = 8

  if(inputFile.length == 0) {
    printUsageMessage()
    sys.exit()
  }

  val nodePairs : List[(Long,Long)] = Source.fromFile(inputFile)
                                         .getLines()
                                         .map(l => l.split("\t"))
                                         .map(fields => (fields(0).toLong, fields(1).toLong))
                                         .flatMap( edge => Seq((edge._1,edge._2),(edge._2,edge._1)))
                                         .toList.
                                          sortWith( (a,b) => { if(a._1 == b._1) a._2 < b._2
                                                               else a._1 < b._1 })
  val degrees = nodePairs.groupBy(edge => edge._1)
                     .map( group => group._1 -> group._2.length.toLong)
                     .toList
                     .sortWith( (a,b) => a._1 < a._2)
                     .map(element => element._2)

  val nodes = degrees.scanLeft(0L)((acc,element) => acc + element).toArray
  val edges = nodePairs.map( pair => pair._2).toArray


}


