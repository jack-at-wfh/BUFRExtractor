package com.weathernexus.utilities.bufr.data

case class BufrCodeFlag(
  fxy: String,
  elementName: String,
  codeFigure: Int,
  entryName: String,
  entryNameSub1: Option[String],
  entryNameSub2: Option[String],
  note: Option[String],
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)

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
