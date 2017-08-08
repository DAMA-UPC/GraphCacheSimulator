package org.dama.graphcachesimulator

import org.dama.graphcachesimulator.types.{CSRGraph, Cache, TwoPhaseCSRGraph}

import scala.collection.mutable
import scala.io.Source

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
    val edges : List[(Int, Int)] = Source.fromFile(inputFile)
      .getLines()
      .map(l => l.split(" "))
      .map(fields => (fields(0).toInt, fields(1).toInt)).toList

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

  def simulateGraph( edges : List[(Int,Int)]) = {

    val graph = CSRGraph(edges)
    //val twoPhaseGraph = TwoPhaseCSRGraph(graph, cacheSizeB / elementSize)

    println("Creating Cache")
    val cache = new Cache(cacheSizeB, cacheLineB, setAssociativity,elementSize)

    println("Simulating accesses")
    for( i <- graph.nodes) {
      val neighbors = graph.neighbors(i)
      neighbors.foreach( neighbor =>
        cache.readElement(neighbor)
      )
    }
    cache.printStats()

    /*val cache2 = new Cache(cacheSizeB, cacheLineB, setAssociativity,elementSize)

    println("Simulating accesses")
    for( i <- twoPhaseGraph.nodes) {
      val neighbors = twoPhaseGraph.neighbors(i)
      neighbors.foreach( neighbor =>
        cache2.readElement(neighbor)
      )
    }
    cache2.printStats()*/
  }
}
