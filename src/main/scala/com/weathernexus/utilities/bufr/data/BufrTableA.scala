package com.weathernexus.utilities.bufr.data

/**
 * Represents a single row of data from a BUFR Table A entry.
 *
 * This case class contains all the information parsed from a row, including the
 * code figure, its English meaning, and its status.
 *
 * @param codeFigure The integer code figure from Table A.
 * @param meaning The English description of the code figure.
 * @param status An optional status string (e.g., "Operational", "Reserved").
 * @param sourceFile The file from which this entry was parsed.
 * @param lineNumber The line number within the source file where this entry was found.
 */
case class BufrTableA(
  codeFigure: Int,
  meaning: String,
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * A type-safe key for uniquely identifying a specific BUFR Table A entry.
 *
 * This case class encapsulates the integer code figure, providing a robust,
 * structured alternative to a simple integer or string key.
 *
 * @param codeFigure The integer code figure associated with the entry.
 */
case class BufrTableAKey(codeFigure: Int) {
  /**
   * Provides a string representation of the key for scenarios that require a simple string.
   * The format is "CodeFigure" (e.g., "0").
   */
  def asString: String = codeFigure.toString
  
  override def toString: String = asString
}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

object BufrTableAKey {
  /**
   * Creates a `BufrTableAKey` from a `BufrTableA` instance.
   *
   * This is a convenient factory method for generating the key directly from the
   * source data object.
   *
   * @param entry The `BufrTableA` instance to create the key from.
   * @return A new `BufrTableAKey` instance.
   */
  def fromEntry(entry: BufrTableA): BufrTableAKey =
    BufrTableAKey(entry.codeFigure)

  /**
   * Attempts to parse a `BufrTableAKey` from its string representation.
   *
   * The expected format is a simple integer string (e.g., "0"). This method safely handles
   * parsing errors and returns `None` if the string is not a valid integer.
   *
   * @param keyStr The string to parse.
   * @return An `Option[BufrTableAKey]` containing the parsed key, or `None` if parsing fails.
   */
  def fromString(keyStr: String): Option[BufrTableAKey] = {
    keyStr.toIntOption.map(BufrTableAKey(_))
  }
}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Represents the detailed data for a specific BUFR Table A entry, excluding the key component.
 *
 * This case class serves as the value in the aggregation map, containing all the descriptive
 * information associated with a `BufrTableAKey`.
 *
 * @param meaning The English description of the code figure.
 * @param status An optional status string.
 * @param sourceFile The file from which this entry was parsed.
 * @param lineNumber The line number within the source file where this entry was found.
 */
case class BufrTableAEntry(
  meaning: String,
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)