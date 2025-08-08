package com.weathernexus.utilities.bufr.descriptors

import zio._
import zio.stream._
import zio.json.*

sealed trait DescriptorCode {
  def f: Int
  def x: Int
  def y: Int
  
  def toFXY: String = f"$f%01d$x%02d$y%03d"
  override def toString: String = toFXY
}

object DescriptorCode {
  case class ElementDescriptor(x: Int, y: Int) extends DescriptorCode {
    val f: Int = 0
  }
  
  case class ReplicationDescriptor(x: Int, y: Int) extends DescriptorCode {
    val f: Int = 1
  }
  
  case class OperatorDescriptor(x: Int, y: Int) extends DescriptorCode {
    val f: Int = 2
  }
  
  case class SequenceDescriptor(x: Int, y: Int) extends DescriptorCode {
    val f: Int = 3
  }

  // JSON codecs
  /**
   * 
   * 
   * given JsonEncoder[DescriptorCode] = JsonEncoder[String].contramap(_.toFXY)
   * given JsonDecoder[DescriptorCode] = JsonDecoder[String].mapOrFail(fromFXY(_).toRight("Invalid FXY code"))
   */

  /**
   * Parses a 6-character string representation of an FXY code into a DescriptorCode.
   *
   * The input string is expected to be in the format "FXXYYY", where:
   * - F is a single digit (0-3, determines descriptor type)
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
    Option.when(fxyCode.length == 6) {
      for {
        f <- fxyCode.substring(0, 1).toIntOption
        x <- fxyCode.substring(1, 3).toIntOption
        y <- fxyCode.substring(3, 6).toIntOption
        descriptor <- f match {
          case 0 => Some(ElementDescriptor(x, y))
          case 1 => Some(ReplicationDescriptor(x, y))
          case 2 => Some(OperatorDescriptor(x, y))
          case 3 => Some(SequenceDescriptor(x, y))
          case _ => None
        }
      } yield descriptor
    }.flatten
  }

  /**
   * Provides a convenient way to create a DescriptorCode from a 6-character FXY string.
   * This is an alias for `fromFXY`.
   */
  def apply(fxyCode: String): Option[DescriptorCode] = fromFXY(fxyCode)
}