package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.data.*

trait BufrTableAAggregator extends Aggregator[BufrTableA, BufrTableAKey, BufrTableAEntry]

object BufrTableAAggregator {
  
  case class Live() extends BufrTableAAggregator {
    
    override def toEntry(element: BufrTableA): BufrTableAEntry =
      BufrTableAEntry(
        meaning = element.meaning,
        status = element.status,
        sourceFile = element.sourceFile,
        lineNumber = element.lineNumber
      )

    override protected def extractKey(element: BufrTableA): BufrTableAKey =
      BufrTableAKey.fromEntry(element)
  }

  val live: ZLayer[Any, Nothing, BufrTableAAggregator] = 
    ZLayer.succeed(Live())

  // Convenience method for direct sink access
  def aggregateToMap(): ZSink[BufrTableAAggregator, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]] =
    ZSink.serviceWithSink[BufrTableAAggregator](_.aggregateToMap())
}