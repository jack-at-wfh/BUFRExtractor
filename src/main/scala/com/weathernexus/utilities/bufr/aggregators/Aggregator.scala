package com.weathernexus.utilities.common.aggregator

import zio.stream.ZSink

// Type parameters:
// T: The input type from the stream (e.g., BufrCodeFlag)
// K: The key type for the map (e.g., BufrCodeFlagKey)
// E: The entry type for the list (e.g., BufrCodeFlagEntry)
trait Aggregator[T, K, E] {

  /**
   * Transforms an input element `T` into its corresponding entry type `E`.
   * This method must be implemented by concrete aggregators.
   */
  def toEntry(element: T): E

  /**
   * ZSink that aggregates a stream of `T` instances into a Map with type-safe keys.
   * Key: K
   * Value: List[E]
   */
  def aggregateToMapTyped(): ZSink[Any, Nothing, T, Nothing, Map[K, List[E]]] =
    baseSink

  /**
   * Must be implemented by concrete aggregators to extract the key from an element.
   */
  protected def extractKey(element: T): K

  protected val baseSink: ZSink[Any, Nothing, T, Nothing, Map[K, List[E]]] =
    ZSink.foldLeft(Map.empty[K, List[E]]) { (acc, element: T) =>
      val key = extractKey(element)
      val entry = toEntry(element)
      val existingEntries = acc.getOrElse(key, List.empty)
      acc + (key -> (entry :: existingEntries))
    }
}