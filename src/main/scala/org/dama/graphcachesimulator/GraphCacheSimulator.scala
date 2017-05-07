package org.dama.graphcachesimulator

import scala.collection.mutable
import scala.io.Source

/**
  * Represents a set in a cache, which implements the LRU evicting policy
  * @param setSize The size of the set (i.e 4)
  */
class CacheSet( val setSize : Int ) {

  var lines = new mutable.ListBuffer[Long]()

  /**
    * Reads the line with the given id and inserts it to the cache if necessary
    * @param tag The tag of the line
    * @return True if the line was in the set
    */
  def read( tag : Long ) : Boolean = {
    val prevNumLines = lines.length
    lines -= tag
    val postNumLines = lines.length
    lines = tag +=: lines
    if(lines.length > setSize) {
      lines = lines.dropRight(1)
    }
    return prevNumLines != postNumLines
  }
}


/**
  * Represents a cache
  * @param cacheSizeB The size of the cache in bytes
  * @param cacheLineB The size of the cache line in bytes
  * @param setAssociativity The associativity of the cache
  * @param elementSizeB The size of each element accessed
  */
class Cache( val cacheSizeB : Int = 32*1024,
             val cacheLineB : Int = 64,
             val setAssociativity : Int = 4,
             val elementSizeB : Int = 8) {

  val numLines = cacheSizeB / cacheLineB
  val numSets = numLines / setAssociativity
  val sets = Array.fill[CacheSet](numSets){new CacheSet(setAssociativity)}
  val elementsPerLine = cacheLineB / elementSizeB
  val statistics = new mutable.HashMap[Long,Long]()
  var min = Long.MaxValue
  var max = Long.MinValue

  def readElement( element : Long ) = {
    val tag = getTag(element)
    val index = getIndex(tag)
    val setId = getSetId(index)
    if(!sets(setId).read(tag)) {
      statistics.put(tag,statistics.getOrElse(tag,0L)+1L)
    }
    min = Math.min(element,min)
    max = Math.max(element,max)
  }

  /**
    * Gets the tag associated with an element
    * @param element The element to get the tag
    * @return The tag of the element
    */
  def getTag( element : Long ) : Long = element / elementsPerLine

  /**
    * Gets the line index of the tag
    * @param tag The tag to get the line index of
    * @return The line index of the tag
    */
  def getIndex( tag : Long ) : Int = (tag % numLines).toInt

  /**
    * Gets the set of a line index
    * @param index The line index to get the set of
    * @return The set of the line index
    */
  def getSetId( index : Int ) : Int = (index % numSets)

  def printStats() = {
    println(s"CacheSize: $cacheSizeB")
    println(s"CacheLineSize: $cacheLineB")
    println(s"NumLines: $numLines")
    println(s"NumSets: $numSets")
    println(s"SetAssociatibity: $setAssociativity")
    val numAccesses = statistics.values.foldLeft(0L)(_+_)
    println("Working set size in number of elements: "+(max - min))
    println("Working set size in lines: "+Math.ceil((max - min)/elementsPerLine.toDouble).toInt)
    println(s"Num of Lines brought from memory: $numAccesses")
  }
}

object GraphCacheSimulator {

  var inputFile = ""
  var cacheSizeB = 32 * 1024
  var cacheLineB = 64
  var setAssociativity = 4
  var elementSize = 8

  def main(args: Array[String]) {

    def printUsageMessage(): Unit = {
      println("--input-file <path> The path to the input graph file")
      println("--cache-size <num> The size of the cache in bytes (default=32768) ")
      println("--line-size <num> The size of the cache line in bytes (default=64)")
      println("--set-associativity <num> The set associativity (default=4)")
      println("--element-size <num> The size of each element (default=8)")
    }

    def nextOption(args: List[String]): Unit = {
      args match {
        case "--input-file" :: value :: tail => {
          inputFile = value
          nextOption(tail)
        }
        case "--cache-size" :: value :: tail => {
          cacheSizeB = value.toInt
          nextOption(tail)
        }
        case "--line-size" :: value :: tail => {
          cacheLineB = value.toInt
          nextOption(tail)
        }
        case "--set-associativity" :: value :: tail => {
          setAssociativity = value.toInt
          nextOption(tail)
        }
        case "--element-size" :: value :: tail => {
          elementSize = value.toInt
          nextOption(tail)
        }
        case value :: tail => Unit
        case Nil => Unit
      }
    }

    nextOption(args.toList)

    if (inputFile.length == 0) {
      printUsageMessage()
      sys.exit()
    }


    println("Reading edge file")
    /** node pairs representing the edges of the graph (repeated in both directions */
    val nodePairs: List[(Long, Long)] = Source.fromFile(inputFile)
      .getLines()
      .map(l => l.split("\t"))
      .map(fields => (fields(0).toLong, fields(1).toLong))
      .flatMap(edge => Seq((edge._1, edge._2), (edge._2, edge._1)))
      .toList.
      sortWith((a, b) => {
        if (a._1 == b._1) a._2 < b._2
        else a._1 < b._1
      })

    val cacheSimulator = new GraphCacheSimulator(cacheSizeB, cacheLineB, setAssociativity, elementSize)
    cacheSimulator.simulateGraph(nodePairs)
  }
}

class GraphCacheSimulator(val cacheSizeB : Int = 32 * 1024,
                          val cacheLineB : Int = 64,
                          val setAssociativity : Int = 4,
                          val elementSize : Int = 8 ) {

  def simulateGraph( nodePairs : List[(Long,Long)]) = {

    println("Building degrees vector")
    /** list of degrees of the nodes of the graph */
    val degrees = nodePairs.groupBy(edge => edge._1)
      .map(group => group._1 -> group._2.length.toLong)
      .toList
      .sortWith((a, b) => a._1 < a._2)
      .map(element => element._2)

    println("Building CSR graph representation")
    /** CSR node array **/
    val nodes : Array[Long] = degrees.scanLeft(0L)((acc, element) => acc + element).dropRight(1).toArray

    /** CSR edge array **/
    val edges : Array[Long]= nodePairs.map(pair => pair._2).toArray

    println("Number of nodes: "+nodes.length)
    println("Number of edges: "+edges.length/2)

    println("Creating Cache")
    val cache = new Cache(cacheSizeB, cacheLineB, setAssociativity,elementSize)

    println("Simulating accesses")
    for( i <- 0 until (nodes.length)) {
      val begin = nodes(i)
      val end = if(i+1 < nodes.length) nodes(i+1)
                else edges.length
      for( j <- begin until end) {
        cache.readElement(edges(j.toInt))
      }
    }
    cache.printStats()
  }
}


