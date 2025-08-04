package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.aggregators.Aggregator
import com.weathernexus.utilities.bufr.data.*

object BufrCodeFlagAggregator extends Aggregator[BufrCodeFlag, BufrCodeFlagKey, BufrCodeFlagEntry] {
  
  override def toEntry(flag: BufrCodeFlag): BufrCodeFlagEntry =
    BufrCodeFlagEntry(
      elementName = flag.elementName,
      entryName = flag.entryName,
      entryNameSub1 = flag.entryNameSub1,
      entryNameSub2 = flag.entryNameSub2,
      note = flag.note,
      status = flag.status,
      sourceFile = flag.sourceFile,
      lineNumber = flag.lineNumber
    )

  /**
   * A `ZSink` that aggregates a stream of `BufrCodeFlag` instances into a `Map`
   * with type-safe `BufrCodeFlagKey` keys.
   *
   * The sink processes a stream of `BufrCodeFlag`s, grouping them by their `fxy` and
   * `codeFigure`. The resulting map's keys are `BufrCodeFlagKey` objects, and the
   * values are lists of `BufrCodeFlagEntry`. The entries within each list are
   * in reverse insertion order (last one seen is first in the list).
   *
   * @return A `ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]]`
   * - `Any`: The sink has no environment dependencies.
   * - `Nothing`: The sink's error channel will not fail.
   * - `BufrCodeFlag`: The sink consumes a stream of `BufrCodeFlag` instances.
   * - `Nothing`: The sink does not emit any intermediate values.
   * - `Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]`: The final aggregated result.
   */
  override def aggregateToMapTyped(): ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]] =
    baseSink

  /**
   * Type-safe version of the aggregation sink that preserves the original insertion order
   * of entries within each list.
   *
   * This sink ensures that the `List` of `BufrCodeFlagEntry` for each key reflects the
   * order in which they were originally consumed from the stream.
   *
   * @return A `ZSink` with type-safe `BufrCodeFlagKey` keys and lists of `BufrCodeFlagEntry`
   * in their original stream order.
   */
  def aggregateToMapTypedPreserveOrder(): ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]] =
    baseSink.map(_.view.mapValues(_.reverse).toMap)

  /**
   * Type-safe version of the aggregation sink that sorts entries by line number.
   *
   * For each key, the `List` of `BufrCodeFlagEntry` is sorted in ascending order
   * based on the `lineNumber` field.
   *
   * @return A `ZSink` with type-safe `BufrCodeFlagKey` keys and sorted lists of entries.
   */
  def aggregateToMapTypedSorted(): ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]] =
    baseSink.map(_.view.mapValues(_.sortBy(_.lineNumber)).toMap)

  override protected def extractKey(element: BufrCodeFlag): BufrCodeFlagKey = 
    BufrCodeFlagKey.fromFlag(element)
}