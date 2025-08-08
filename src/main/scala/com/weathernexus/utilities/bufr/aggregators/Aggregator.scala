package com.weathernexus.utilities.bufr.aggregators

import zio.stream.ZSink

trait Aggregator[T, K, E] {
  def toEntry(element: T): E
  def aggregateToMap(): ZSink[Any, Nothing, T, Nothing, Map[K, List[E]]] = baseSink
  protected def extractKey(element: T): K

  // Common base sink implementation
  protected val baseSink: ZSink[Any, Nothing, T, Nothing, Map[K, List[E]]] =
    ZSink.foldLeft(Map.empty[K, List[E]]) { (acc, element: T) =>
      val key = extractKey(element)
      val entry = toEntry(element)
      val existingEntries = acc.getOrElse(key, List.empty)
      acc + (key -> (entry :: existingEntries))
    }
}
