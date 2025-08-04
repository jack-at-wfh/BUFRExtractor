package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.descriptors.DescriptorCode

object BufrTableBAggregator extends Aggregator[BufrTableB, DescriptorCode, BufrTableBEntry] {

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

  /**
   * A `ZSink` that aggregates a stream of `BufrTableB` instances into a `Map`
   * keyed by `DescriptorCode`.
   *
   * The sink processes a stream of `BufrTableB`s, grouping them by their `fxy` code.
   * The resulting map's keys are `DescriptorCode` objects, and the values are lists
   * of `BufrTableBEntry`. The entries within each list are in reverse insertion order
   * (last one seen is first in the list).
   *
   * @return A `ZSink[Any, Nothing, BufrTableB, Nothing, Map[DescriptorCode, List[BufrTableBEntry]]]`
   * - `Any`: The sink has no environment dependencies.
   * - `Nothing`: The sink's error channel will not fail.
   * - `BufrTableB`: The sink consumes a stream of `BufrTableB` instances.
   * - `Nothing`: The sink does not emit any intermediate values.
   * - `Map[DescriptorCode, List[BufrTableBEntry]]`: The final aggregated result.
   */
  override def aggregateToMapTyped(): ZSink[Any, Nothing, BufrTableB, Nothing, Map[DescriptorCode, List[BufrTableBEntry]]] =
    baseSink

  /**
   * Type-safe version of the aggregation sink that preserves the original insertion order
   * of entries within each list.
   *
   * This sink ensures that the `List` of `BufrTableBEntry` for each key reflects the
   * order in which they were originally consumed from the stream.
   *
   * @return A `ZSink` with type-safe `DescriptorCode` keys and lists of `BufrTableBEntry`
   * in their original stream order.
   */
  def aggregateToMapTypedPreserveOrder(): ZSink[Any, Nothing, BufrTableB, Nothing, Map[DescriptorCode, List[BufrTableBEntry]]] =
    baseSink.map(_.view.mapValues(_.reverse).toMap)

  /**
   * Type-safe version of the aggregation sink that sorts entries by line number.
   *
   * For each key, the `List` of `BufrTableBEntry` is sorted in ascending order
   * based on the `lineNumber` field.
   *
   * @return A `ZSink` with type-safe `DescriptorCode` keys and sorted lists of entries.
   */
  def aggregateToMapTypedSorted(): ZSink[Any, Nothing, BufrTableB, Nothing, Map[DescriptorCode, List[BufrTableBEntry]]] =
    baseSink.map(_.view.mapValues(_.sortBy(_.lineNumber)).toMap)

  override protected def extractKey(element: BufrTableB): DescriptorCode =
    DescriptorCode.fromFXY(element.fxy).getOrElse {
      throw new IllegalArgumentException(s"Invalid FXY code: ${element.fxy}")
  }
}