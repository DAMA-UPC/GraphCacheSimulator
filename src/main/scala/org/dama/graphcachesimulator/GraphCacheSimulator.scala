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
    prevNumLines != postNumLines
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

  def readElement( element : Long ) : Boolean = {
    val tag = getTag(element)
    val index = getIndex(tag)
    val setId = getSetId(index)
    val hit = sets(setId).read(tag)
    val miss = !hit
    if(miss)
      statistics.put(tag,statistics.getOrElse(tag,0L)+1L)
    min = Math.min(element,min)
    max = Math.max(element,max)
    hit
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
    println("Working set size in number of elements: "+((max - min)+1))
    println("Working set size in lines: "+Math.ceil(((max - min)+1)/elementsPerLine.toDouble).toInt)
    println("Num of different lines touched "+statistics.values.size)
    println(s"Num of Lines brought from memory: $numAccesses")
  }
}

object GraphCacheSimulator {

  var inputFile = ""
  var cacheSizeB = 32 * 1024
  var cacheLineB = 64
  var setAssociativity = 4
  var elementSize = 8

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

  def main(args: Array[String]) {

    nextOption(args.toList)

    if (inputFile.length == 0) {
      printUsageMessage()
      sys.exit()
    }

    println("Reading edge file")
    /** node pairs representing the edges of the graph (repeated in both directions */
    val edges : List[(Long, Long)] = Source.fromFile(inputFile)
      .getLines()
      .map(l => l.split(" "))
      .map(fields => (fields(0).toLong, fields(1).toLong)).toList

    val duplicatedEdges =  edges.flatMap( { case (tail,head) => {
        val edge1 = (tail, head)
        val edge2 = (head, tail)
        Seq(edge1,edge2)}})

    val cacheSimulator = new GraphCacheSimulator(cacheSizeB, cacheLineB, setAssociativity, elementSize)
    cacheSimulator.simulateGraph(duplicatedEdges)
  }
}

class GraphCacheSimulator( val cacheSizeB : Int = 32 * 1024,
                           val cacheLineB : Int = 64,
                           val setAssociativity : Int = 4,
                           val elementSize : Int = 8 ) {

  def simulateGraph( edges : List[(Long,Long)]) = {

    println("Building degrees vector")
    val sortedEdges = edges.sortBy({ case (tail,head) => tail })
    /** list of degrees of the nodes of the graph */
    val nodeDegrees = sortedEdges.groupBy( {case (tail,head) => tail} )
                               .map( { case (node, neighbors) => node -> neighbors.length.toLong })
                               .toList

    val degrees = nodeDegrees
      .sortBy( { case (node, degree) => node })
      .map( { case (node, degree) => degree})

    println("Building CSR graph representation")
    /** CSR node array **/
    val csrNodes : Array[Long] = degrees.scanLeft(0L)((acc, element) => acc + element).dropRight(1).toArray

    /** CSR edge array **/
    val csrEdges : Array[Long]= sortedEdges.map( {case (tail,head) => head }).toArray

    println("Number of nodes: "+csrNodes.length)
    println("Number of edges: "+csrEdges.length/2)

    println("Creating Cache")
    val cache = new Cache(cacheSizeB, cacheLineB, setAssociativity,elementSize)

    println("Simulating accesses")
    for( i <- 0 until (csrNodes.length)) {
      val begin = csrNodes(i)
      val end = if(i+1 < csrNodes.length) csrNodes(i+1)
                else csrEdges.length
      for( j <- begin until end) {
        cache.readElement(csrEdges(j.toInt))
      }
    }
    cache.printStats()
  }
}
