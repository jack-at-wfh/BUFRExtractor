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
  override def aggregateToMap(): ZSink[Any, Nothing, BufrTableA, Nothing, Map[BufrTableAKey, List[BufrTableAEntry]]] =
    baseSink

  override protected def extractKey(element: BufrTableA): BufrTableAKey =
    BufrTableAKey.fromEntry(element)
}