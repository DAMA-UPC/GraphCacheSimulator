package org.dama.graphcachesimulator.types

import scala.collection.mutable

/**
  * Represents a cache
 *
  * @param cacheSizeB The size of the cache in bytes
  * @param cacheLineB The size of the cache line in bytes
  * @param setAssociativity The associativity of the cache
  * @param elementSizeB The size of each element accessed
  */
class Cache( val cacheSizeB : Int = 32*1024,
             val cacheLineB : Int = 64,
             val setAssociativity : Int = 4,
             val elementSizeB : Int = 4) {

  val numLines = cacheSizeB / cacheLineB
  val numSets = numLines / setAssociativity
  val sets = Array.fill[CacheSet](numSets){new CacheSet(setAssociativity)}
  val elementsPerLine = cacheLineB / elementSizeB
  val statistics = new mutable.HashMap[Long,Long]()
  var min = Long.MaxValue
  var max = Long.MinValue
  var reads = 0L
  var misses = 0L

  def readElement( element : Long ) : Boolean = {
    val tag = getTag(element)
    val index = getIndex(tag)
    val setId = getSetId(index)
    val hit = sets(setId).read(tag)
    val miss = !hit
    reads += 1L
    if(miss) {
      statistics.put(tag, statistics.getOrElse(tag, 0L) + 1L)
      misses += 1L
    }
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
    val workingSetSizeInElements = ((max - min)+1)
    val workingSetSizeInLines = Math.ceil(((max - min)+1)/elementsPerLine.toDouble).toInt
    val missRate = (misses/reads.toDouble)
    val optimalMissRate = (workingSetSizeInLines/reads.toDouble)
    println("Number of cache accesses:"+reads)
    println("Number of cache misses:"+misses)
    println(s"Observed Missrate:$missRate")
    println(s"Optimal Missrate:$optimalMissRate")
    println(s"Working set size in number of elements:$workingSetSizeInElements")
    println(s"Working set size in lines:$workingSetSizeInLines")
    println("Num of different lines touched:"+statistics.values.size)
    println(s"Num of Lines brought from memory:$numAccesses")
  }
}
