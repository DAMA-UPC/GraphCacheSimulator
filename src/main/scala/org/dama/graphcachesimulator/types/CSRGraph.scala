package org.dama.graphcachesimulator.types

/**
  * Created by aprat on 11/05/17.
  */

object CSRGraph {

  def apply(edges : List[(Int,Int)]) : CSRGraph = {

    println("Building degrees vector")
    val sortedEdges : List[(Int,Int)] = edges.sortBy({ case (tail,head) => tail })
    /** list of degrees of the nodes of the graph */
    val nodeDegrees : List[(Int,Int)] = sortedEdges.groupBy( {case (tail,head) => tail} )
      .map( { case (node, neighbors) => node -> neighbors.length })
      .toList

    val degrees : List[Int] = nodeDegrees
      .sortBy( { case (node, degree) => node })
      .map( { case (node, degree) => degree})

    println("Building CSR graph representation")
    /** CSR node array **/
    val csrNodes : Array[Int] = degrees.scanLeft(0)((acc, element) => acc + element).dropRight(1).toArray

    /** CSR edge array **/
    val csrEdges : Array[Int]= sortedEdges.map( {case (tail,head) => head }).toArray

    println("Number of nodes: "+csrNodes.length)
    println("Number of edges: "+csrEdges.length/2)

    new CSRGraph(csrNodes,csrEdges)
  }
}

class CSRGraph( csrNodes : Array[Int], csrEdges : Array[Int] ) {

  def numNodes : Int = csrNodes.length

  def nodes : Range = Range(0,csrNodes.length)

  def neighbors( node : Int  ) : Array[Int] = {
    val begin  = csrNodes(node)
    val end = if(node+1 < csrNodes.length) csrNodes(node+1)
    else csrEdges.length
    csrEdges.slice(begin.toInt,end.toInt)
  }

}
