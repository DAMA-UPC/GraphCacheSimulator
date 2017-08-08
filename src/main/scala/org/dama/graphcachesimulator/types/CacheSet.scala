package org.dama.graphcachesimulator.types

import scala.collection.mutable

/**
  * Represents a set in a cache, which implements the LRU evicting policy
 *
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



