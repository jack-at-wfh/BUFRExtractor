package com.bufrtools.descriptors

import zio._
import zio.stream._
import java.io.InputStream
import scala.util.Using

// Descriptor code representation
case class DescriptorCode(f: Int, x: Int, y: Int) {
  def toFXY: String = f"$f%01d$x%02d$y%03d"
  
  def isElementDescriptor: Boolean = f == 0
  def isReplicationDescriptor: Boolean = f == 1
  def isOperatorDescriptor: Boolean = f == 2
  def isSequenceDescriptor: Boolean = f == 3
}

// Companion object for DescriptorCode, providing factory methods.
object DescriptorCode {
  /**
   * Parses a 6-character string representation of an FXY code into a DescriptorCode.
   *
   * The input string is expected to be in the format "FXXYYY", where:
   * - F is a single digit (octet 0)
   * - X is two digits (octets 1-2)
   * - Y is three digits (octets 3-5)
   *
   * Returns Some(DescriptorCode) if the string has the correct length (6 characters)
   * and all parts can be successfully parsed as integers. Otherwise, returns None.
   *
   * @param fxyCode The 6-character string representing the FXY code.
   * @return An Option containing the parsed DescriptorCode, or None if parsing fails or length is incorrect.
   */
  def fromFXY(fxyCode: String): Option[DescriptorCode] = {
    // Use Option.when to only proceed if the length is exactly 6.
    // If the length is not 6, Option.when returns None immediately.
    Option.when(fxyCode.length == 6) {
      // Use a for-comprehension over Option to safely parse each part.
      // String.toIntOption returns Some(Int) on success, None on NumberFormatException.
      // If any of these parsing steps results in None, the entire for-comprehension
      // will short-circuit and return None.
      for {
        f <- fxyCode.substring(0, 1).toIntOption // F is single digit (e.g., "0" from "012345")
        x <- fxyCode.substring(1, 3).toIntOption // X is 2 digits (e.g., "12" from "012345")
        y <- fxyCode.substring(3, 6).toIntOption // Y is 3 digits (e.g., "345" from "012345")
      } yield DescriptorCode(f, x, y)
    }.flatten // Flatten Option[Option[DescriptorCode]] to Option[DescriptorCode]
  }

  /**
   * Provides a convenient way to create a DescriptorCode from a 6-character FXY string.
   * This is an alias for `fromFXY`.
   *
   * Example:
   * `DescriptorCode("001002")` will attempt to parse the code.
   *
   * @param fxyCode The 6-character string representing the FXY code.
   * @return An Option containing the parsed DescriptorCode, or None if parsing fails or length is incorrect.
   */
  def apply(fxyCode: String): Option[DescriptorCode] = fromFXY(fxyCode)
}