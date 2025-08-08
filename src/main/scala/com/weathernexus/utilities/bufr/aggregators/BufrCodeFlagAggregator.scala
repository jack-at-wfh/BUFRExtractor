package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.aggregators.Aggregator
import com.weathernexus.utilities.bufr.data.*

trait BufrCodeFlagAggregator extends Aggregator[BufrCodeFlag, BufrCodeFlagKey, BufrCodeFlagEntry]

object BufrCodeFlagAggregator {
  case class Live() extends BufrCodeFlagAggregator {
    override def toEntry(element: BufrCodeFlag): BufrCodeFlagEntry =
      BufrCodeFlagEntry(
        elementName = element.elementName,
        entryName = element.entryName,
        entryNameSub1 = element.entryNameSub1,
        entryNameSub2 = element.entryNameSub2,
        note = element.note,
        status = element.status,
        sourceFile = element.sourceFile,
        lineNumber = element.lineNumber
      )

    override protected def extractKey(element: BufrCodeFlag): BufrCodeFlagKey =
      BufrCodeFlagKey.fromFlag(element)
  }

  val live: ZLayer[Any, Nothing, BufrCodeFlagAggregator] = 
    ZLayer.succeed(Live())

  def aggregateToMap(): ZSink[BufrCodeFlagAggregator, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]] =
    ZSink.serviceWithSink[BufrCodeFlagAggregator](_.aggregateToMap())
}