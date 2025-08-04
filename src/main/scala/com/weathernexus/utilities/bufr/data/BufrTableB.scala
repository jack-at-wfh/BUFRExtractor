package com.weathernexus.utilities.bufr.data

/**
 * Represents a single row of data from a BUFR Table B entry.
 *
 * This case class contains all the information parsed from a row, including the
 * FXY descriptor, its English name, units, scale, reference value, and data width.
 *
 * @param classNumber The class number (e.g., "00" for "BUFR/CREX table entries").
 * @param className The name of the class.
 * @param fxy The full FXY descriptor string (e.g., "001001").
 * @param elementName The English description of the element.
 * @param note An optional note associated with the entry.
 * @param bufrUnit The BUFR unit (e.g., "Numeric", "CCITT IA5", "Code table").
 * @param bufrScale The BUFR scale factor.
 * @param bufrReferenceValue The BUFR reference value.
 * @param bufrDataWidth The BUFR data width in bits.
 * @param crexUnit An optional CREX unit.
 * @param crexScale An optional CREX scale factor.
 * @param crexDataWidth An optional CREX data width in characters.
 * @param status An optional status string (e.g., "Operational").
 * @param sourceFile The file from which this entry was parsed.
 * @param lineNumber The line number within the source file where this entry was found.
 */
case class BufrTableB(
  classNumber: Int,
  className: String,
  fxy: String,
  elementName: String,
  note: Option[String],
  bufrUnit: String,
  bufrScale: Int,
  bufrReferenceValue: Int,
  bufrDataWidth: Int,
  crexUnit: Option[String],
  crexScale: Option[Int],
  crexDataWidth: Option[Int],
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * A type-safe key for uniquely identifying a specific BUFR Table B entry.
 *
 * This case class encapsulates the FXY descriptor, providing a robust, structured
 * alternative to a simple string key.
 *
 * @param fxy The FXY descriptor string (e.g., "001001") associated with the entry.
 */
case class BufrTableBKey(fxy: String) {
  /**
   * Provides a string representation of the key for scenarios that require a simple string.
   * The format is "FXY" (e.g., "001001").
   */
  def asString: String = fxy

  override def toString: String = asString
}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

object BufrTableBKey {
  /**
   * Creates a `BufrTableBKey` from a `BufrTableB` instance.
   *
   * This is a convenient factory method for generating the key directly from the
   * source data object.
   *
   * @param entry The `BufrTableB` instance to create the key from.
   * @return A new `BufrTableBKey` instance.
   */
  def fromEntry(entry: BufrTableB): BufrTableBKey =
    BufrTableBKey(entry.fxy)

  /**
   * Attempts to parse a `BufrTableBKey` from its string representation.
   *
   * The expected format is a simple FXY string (e.g., "001001"). This method assumes the
   * string is already in the correct format as it is often a critical identifier.
   *
   * @param keyStr The string to parse.
   * @return An `Option[BufrTableBKey]` containing the parsed key, or `None` if the format is invalid.
   */
  def fromString(keyStr: String): Option[BufrTableBKey] = {
    // FXY is a 6-digit string, so we can do a simple check.
    if (keyStr.length == 6) Some(BufrTableBKey(keyStr)) else None
  }
}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Represents the detailed data for a specific BUFR Table B entry, excluding the key component.
 *
 * This case class serves as the value in the aggregation map, containing all the descriptive
 * information associated with a `BufrTableBKey`.
 *
 * @param classNumber The class number.
 * @param className The name of the class.
 * @param elementName The English description of the element.
 * @param note An optional note.
 * @param bufrUnit The BUFR unit.
 * @param bufrScale The BUFR scale factor.
 * @param bufrReferenceValue The BUFR reference value.
 * @param bufrDataWidth The BUFR data width in bits.
 * @param crexUnit An optional CREX unit.
 * @param crexScale An optional CREX scale factor.
 * @param crexDataWidth An optional CREX data width in characters.
 * @param status An optional status string.
 * @param sourceFile The file from which this entry was parsed.
 * @param lineNumber The line number within the source file where this entry was found.
 */
case class BufrTableBEntry(
  classNumber: Int,
  className: String,
  elementName: String,
  note: Option[String],
  bufrUnit: String,
  bufrScale: Int,
  bufrReferenceValue: Int,
  bufrDataWidth: Int,
  crexUnit: Option[String],
  crexScale: Option[Int],
  crexDataWidth: Option[Int],
  status: Option[String],
  sourceFile: String,
  lineNumber: Int
)