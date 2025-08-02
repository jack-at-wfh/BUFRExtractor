package com.weathernexus.utilities.bufr.aggregators

import zio.*
import zio.stream.*

import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.common.aggregator.Aggregator
/**
 * A type-safe key for uniquely identifying a specific BUFR code or flag entry.
 *
 * This case class encapsulates the FXY descriptor string and the CodeFigure integer,
 * providing a robust, structured alternative to a simple concatenated string key.
 *
 * @param fxy The BUFR FXY descriptor string (e.g., "001001").
 * @param codeFigure The integer code or flag value associated with the FXY descriptor.
 */
case class BufrCodeFlagKey(fxy: String, codeFigure: Int) {
  /**
 * Provides a string representation of the key for scenarios that require a simple string,
 * such as file formats or legacy systems.
 * The format is "FXY-CodeFigure" (e.g., "001001-0").
 */
  def asString: String = s"$fxy-$codeFigure"
  
  override def toString: String = asString
}

object BufrCodeFlagKey {
  /**
   * Creates a `BufrCodeFlagKey` from a `BufrCodeFlag` instance.
   *
   * This is a convenient factory method for generating the key directly from the
   * source data object.
   *
   * @param flag The `BufrCodeFlag` instance to create the key from.
   * @return A new `BufrCodeFlagKey` instance.
   */
  def fromFlag(flag: BufrCodeFlag): BufrCodeFlagKey = 
    BufrCodeFlagKey(flag.fxy, flag.codeFigure)
  
  /**
   * Attempts to parse a `BufrCodeFlagKey` from its string representation.
   *
   * The expected format is "FXY-CodeFigure" (e.g., "001001-0"). This method safely handles
   * parsing errors and returns `None` if the string is not in the correct format or
   * if the code figure is not a valid integer.
   *
   * @param keyStr The string to parse.
   * @return An `Option[BufrCodeFlagKey]` containing the parsed key, or `None` if parsing fails.
   */
  def fromString(keyStr: String): Option[BufrCodeFlagKey] = {
    keyStr.split("-", 2) match {
      case Array(fxy, codeStr) => 
        codeStr.toIntOption.map(BufrCodeFlagKey(fxy, _))
      case _ => None
    }
  }
}

/**
 * Represents the detailed data for a specific BUFR code flag, excluding the key components.
 *
 * This case class serves as the value in the aggregation map, containing all the descriptive
 * information associated with a `BufrCodeFlagKey`.
 *
 * @param elementName A descriptive name for the element.
 * @param entryName The primary name of the code flag entry.
 * @param entryNameSub1 An optional sub-name for the entry.
 * @param entryNameSub2 Another optional sub-name for the entry.
 * @param note An optional note providing additional context.
 * @param status An optional status string.
 * @param sourceFile The file from which this entry was parsed.
 * @param lineNumber The line number within the source file where this entry was found.
 */
case class BufrCodeFlagEntry(
  elementName: String,
  entryName: String,
  entryNameSub1: Option[String],
  entryNameSub2: Option[String],
  note: Option[String],
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)

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

  // // Abstract the core fold logic with type-safe keys
  // private val baseSinkTyped: ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]]] =
  //   ZSink.foldLeft(Map.empty[BufrCodeFlagKey, List[BufrCodeFlagEntry]]) { (acc, flag: BufrCodeFlag) =>
  //     val key = BufrCodeFlagKey.fromFlag(flag)
  //     val entry = toEntry(flag)
  //     val existingEntries = acc.getOrElse(key, List.empty)
  //     acc + (key -> (entry :: existingEntries))
  //   }

  // // Abstract the core fold logic with string keys (for backwards compatibility)
  // private val baseSink: ZSink[Any, Nothing, BufrCodeFlag, Nothing, Map[String, List[BufrCodeFlagEntry]]] =
  //   baseSinkTyped.map(_.map { case (key, entries) => key.asString -> entries })

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