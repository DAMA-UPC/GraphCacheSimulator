package org.dama.graphcachesimulator

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by aprat on 7/05/17.
  */
class GraphCacheSimulatorTest extends FlatSpec with Matchers  {

  "" should "" in {

    val nodePairs : List[(Long,Long)] = List((0L,1L),(1L,0L))
    val cacheSimulator = new GraphCacheSimulator()
    cacheSimulator.simulateGraph(nodePairs)
  }

  "A Cache Set of size 4 with LRU replacement policy " should " always miss after sequential access to 5 elements " in {

    val cacheSet = new CacheSet(4)

    for( i <- 0L until 5L) {
      cacheSet.read(i) should be (false)
    }

    for( i <- 0L until 5L) {
      cacheSet.read(i) should be (false)
    }
  }

  "A Cache Set of size 4 with LRU replacement policy " should " first miss after sequential access to 4 elements and then hit after sequential access to the same 4 elements" in {

    val cacheSet = new CacheSet(4)

    for( i <- 0L until 4L) {
      cacheSet.read(i) should be (false)
    }

    for( i <- 0L until 4L) {
      cacheSet.read(i) should be (true)
    }
  }

  "In a Cache with default parameters, tag of elements 0, 1024, 2048, 4096 and 32768" should " be 0, 128, 256 and 512, 4096 " in {

    val cache = new Cache()
    cache.getTag(0L) should be (0)
    cache.getTag(1024L) should be (128)
    cache.getTag(2048L) should be (256)
    cache.getTag(4096L) should be (512)
    cache.getTag(32768L) should be (4096)

  }

  "In a Cache with default parameters, index of elements 0, 1024, 2048, 4096 and 32768" should " be 0, 128, 256, 0 and 0 " in {

    val cache = new Cache()
    cache.getIndex(cache.getTag(0L)) should be (0)
    cache.getIndex(cache.getTag(1024L)) should be (128)
    cache.getIndex(cache.getTag(2048L)) should be (256)
    cache.getIndex(cache.getTag(4096L)) should be (0)
    cache.getIndex(cache.getTag(32768L)) should be (0)

  }

  "In a Cache with default parameters, setIds of elements 0, 1024, 2048, 4096 and 32768" should " 0, 0, 0 and 0 " in {

    val cache = new Cache()
    cache.getSetId(cache.getIndex(cache.getTag(0L))) should be (0)
    cache.getSetId(cache.getIndex(cache.getTag(1024L))) should be (0)
    cache.getSetId(cache.getIndex(cache.getTag(2048L))) should be (0)
    cache.getSetId(cache.getIndex(cache.getTag(4096L))) should be (0)
    cache.getSetId(cache.getIndex(cache.getTag(32768L))) should be (0)

  }
}
