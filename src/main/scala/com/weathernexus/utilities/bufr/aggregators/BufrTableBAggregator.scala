package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.descriptors.DescriptorCode

trait BufrTableBAggregator extends Aggregator[BufrTableB, BufrTableBKey, BufrTableBEntry]

object BufrTableBAggregator {
  case class Live() extends BufrTableBAggregator {

    override def toEntry(data: BufrTableB): BufrTableBEntry = 
      BufrTableBEntry(
        classNumber = data.classNumber,
        className = data.className,
        // Removed the 'fxy' line, as it's not a parameter of BufrTableBEntry
        elementName = data.elementName,
        note = data.note,
        bufrUnit = data.bufrUnit,
        bufrScale = data.bufrScale,
        bufrReferenceValue = data.bufrReferenceValue,
        bufrDataWidth = data.bufrDataWidth,
        crexUnit = data.crexUnit,
        crexScale = data.crexScale,
        crexDataWidth = data.crexDataWidth,
        status = data.status,
        sourceFile = data.sourceFile,
        lineNumber = data.lineNumber
      )

    override protected def extractKey(element: BufrTableB): BufrTableBKey = 
      BufrTableBKey.fromEntry(element)

  }

  val live: ZLayer[Any, Nothing, BufrTableBAggregator] = 
    ZLayer.succeed(Live())

  def aggregateToMap(): ZSink[BufrTableBAggregator, Nothing, BufrTableB, Nothing, Map[BufrTableBKey, List[BufrTableBEntry]]] =
    ZSink.serviceWithSink[BufrTableBAggregator](_.aggregateToMap())
}