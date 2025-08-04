package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.data.*

object BufrTableAAggregator extends Aggregator[BufrTableA, BufrTableAKey, BufrTableAEntry] {

  override def toEntry(flag: BufrTableA): BufrTableAEntry =
    BufrTableAEntry(
      meaning = flag.meaning,
      status = flag.status,
      sourceFile = flag.sourceFile,
      lineNumber = flag.lineNumber
    )

  /**
   * A `ZSink` that aggregates a stream of `BufrTableA` instances into a `Map`
   * with type-safe `BufrTableAKey` keys.
   *
   * The sink processes a stream of `BufrTableA`s, grouping them by their `codeFigure`.
   * The resulting map's keys are `BufrTableAKey` objects, and the values are lists
   * of `BufrTableAEntry`. The entries within each list are in reverse insertion order
   * (last one seen is first in the list).
   *
   * @return A `ZSink[Any, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]]`
   * - `Any`: The sink has no environment dependencies.
   * - `Nothing`: The sink's error channel will not fail.
   * - `BufrTableA`: The sink consumes a stream of `BufrTableA` instances.
   * - `Nothing`: The sink does not emit any intermediate values.
   * - `Map[BufrTableAKey, List[BufrTableAEntry]]`: The final aggregated result.
   */
  override def aggregateToMapTyped(): ZSink[Any, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]] =
    baseSink

  /**
   * Type-safe version of the aggregation sink that preserves the original insertion order
   * of entries within each list.
   *
   * This sink ensures that the `List` of `BufrTableAEntry` for each key reflects the
   * order in which they were originally consumed from the stream.
   *
   * @return A `ZSink` with type-safe `BufrTableAKey` keys and lists of `BufrTableAEntry`
   * in their original stream order.
   */
  def aggregateToMapTypedPreserveOrder(): ZSink[Any, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]] =
    baseSink.map(_.view.mapValues(_.reverse).toMap)

  /**
   * Type-safe version of the aggregation sink that sorts entries by line number.
   *
   * For each key, the `List` of `BufrTableAEntry` is sorted in ascending order
   * based on the `lineNumber` field.
   *
   * @return A `ZSink` with type-safe `BufrTableAKey` keys and sorted lists of entries.
   */
  def aggregateToMapTypedSorted(): ZSink[Any, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]] =
    baseSink.map(_.view.mapValues(_.sortBy(_.lineNumber)).toMap)

  override protected def extractKey(element: BufrTableA): BufrTableAKey =
    BufrTableAKey.fromEntry(element)
}