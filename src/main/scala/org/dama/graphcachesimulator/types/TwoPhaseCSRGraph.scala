package org.dama.graphcachesimulator.types

import scala.collection.mutable
import scala.util.Random

/**
  * Created by aprat on 11/05/17.
  */

object TwoPhaseCSRGraph {

  def apply( csrGraph : CSRGraph, cacheSize : Int ) : CSRGraph = {
    val frontierSize = cacheSize / 2
    val visited = mutable.HashSet[Int]()

    var bfsOrder = mutable.ListBuffer[Int]()
    var edges = mutable.ListBuffer[(Int,Int)]()
    var outEdges = mutable.ListBuffer[(Int,Int)]()

    var currentFrontier = new mutable.HashSet[Int]()
    var nextFrontier = new mutable.HashSet[Int]()
    for(node <- csrGraph.nodes) {
      if (!visited(node)) {
        currentFrontier.clear
        nextFrontier.clear
        currentFrontier.add(node)
        visited.add(node)
        while (currentFrontier.nonEmpty) {
          val candidates = mutable.HashSet[(Int,Int,Int)]()
          for (next <- currentFrontier) {
            bfsOrder.append(next)
            val neighbors = csrGraph.neighbors(next)
            for (neighbor <- neighbors) {
              /*if (!visited(neighbor) && nextFrontier.size < frontierSize) {
                nextFrontier.add(neighbor)
                visited.add(neighbor)
                edges.append(Tuple2[Int,Int](next, neighbor), Tuple2[Int,Int](neighbor, next))
              } else if (currentFrontier.contains(neighbor)) {
                edges.append(Tuple2[Int,Int](next, neighbor))
              }*/
              if (!visited(neighbor)) {
                candidates.add((next,neighbor,csrGraph.neighbors(neighbor).size))
              } else if (currentFrontier.contains(neighbor)) {
                edges.append(Tuple2[Int,Int](next, neighbor))
              } else {
                outEdges.append(Tuple2[Int,Int](next, neighbor))
              }
            }
          }
          val nextBlock=candidates.toList.sortBy( { case (tail,head,degree) => degree } ).slice(0,frontierSize)
          nextBlock.foreach( {case (tail, head, _) =>
            nextFrontier.add(head)
            visited.add(head)
            edges.append(Tuple2[Int,Int](tail, head), Tuple2[Int,Int](head, tail))
          })

          val tmp = currentFrontier
          currentFrontier = nextFrontier
          nextFrontier = tmp
          nextFrontier.clear()
        }
      }
    }
    edges = edges.distinct
    println(edges.size)
    outEdges = outEdges.distinct
    println(outEdges.size)
    var i = -1
    val mapping = bfsOrder.map( node => {
      i+=1
      node -> i
    }).toMap
    println(bfsOrder.size)
    val newedges = edges.map( {case (tail,head) => (mapping(tail),mapping(head))})
    CSRGraph(newedges.toList)
  }
}

