package com.weathernexus.utilities.bufr.parsers.section

import zio._
import zio.stream._

// BUFR Section 2 structure (optional section for local use)
case class BufrSection2(
  sectionLength: Int,        // Length of section in bytes (3 bytes)
  reserved: Int,             // Reserved byte (should be 0) (1 byte)
  localData: Chunk[Byte]     // Local use data (remaining bytes)
)

// Custom errors for Section 2 parsing
sealed trait BufrSection2ParseError extends Exception
case class Section2InsufficientData(expected: Int, actual: Int) extends BufrSection2ParseError {
  override def getMessage: String = s"Section 2: Expected $expected bytes, got $actual"
}
case class Section2InvalidLength(length: Int, minExpected: Int) extends BufrSection2ParseError {
  override def getMessage: String = s"Section 2: Invalid length $length, minimum expected $minExpected"
}
case class Section2InvalidReservedByte(value: Int) extends BufrSection2ParseError {
  override def getMessage: String = s"Section 2: Reserved byte should be 0, got $value"
}

object BufrSection2Parser {
  
  // Section 2 minimum length is 4 bytes (3 for length + 1 reserved)
  private val SECTION2_MIN_LENGTH = 4
  
  // Parse Section 2 from a Chunk[Byte]
  def parseSection2(data: Chunk[Byte]): IO[BufrSection2ParseError, BufrSection2] = {
    for {
      _              <- validateMinimumLength(data)
      sectionLength  <- extractSectionLength(data)
      _              <- validateSectionLength(sectionLength, data.length)
      section2       <- extractSection2Fields(data, sectionLength)
      _              <- validateReservedByte(section2)
    } yield section2
  }
  
  // Create a ZIO Stream transformer
  def parseSection2Stream: ZPipeline[Any, BufrSection2ParseError, Chunk[Byte], BufrSection2] =
    ZPipeline.mapZIO(parseSection2)
  
  private def validateMinimumLength(data: Chunk[Byte]): IO[BufrSection2ParseError, Unit] = {
    if (data.length < SECTION2_MIN_LENGTH)
      ZIO.fail(Section2InsufficientData(SECTION2_MIN_LENGTH, data.length))
    else
      ZIO.unit
  }
  
  private def extractSectionLength(data: Chunk[Byte]): IO[Nothing, Int] =
    ZIO.succeed {
      val bytes = data.take(3).toArray
      // Convert 3 bytes to int (big-endian)
      ((bytes(0) & 0xFF) << 16) | 
      ((bytes(1) & 0xFF) << 8) | 
      (bytes(2) & 0xFF)
    }
  
  private def validateSectionLength(sectionLength: Int, dataLength: Int): IO[BufrSection2ParseError, Unit] = {
    if (sectionLength < SECTION2_MIN_LENGTH)
      ZIO.fail(Section2InvalidLength(sectionLength, SECTION2_MIN_LENGTH))
    else if (sectionLength > dataLength)
      ZIO.fail(Section2InsufficientData(sectionLength, dataLength))
    else
      ZIO.unit
  }
  
  private def extractSection2Fields(data: Chunk[Byte], sectionLength: Int): IO[Nothing, BufrSection2] =
    ZIO.succeed {
      val bytes = data.toArray
      
      val reserved = bytes(3) & 0xFF
      val localDataLength = sectionLength - 4  // Total length minus header (3 bytes length + 1 byte reserved)
      val localData = if (localDataLength > 0) {
        data.drop(4).take(localDataLength)
      } else {
        Chunk.empty[Byte]
      }
      
      BufrSection2(
        sectionLength = sectionLength,
        reserved = reserved,
        localData = localData
      )
    }
  
  private def validateReservedByte(section2: BufrSection2): IO[BufrSection2ParseError, Unit] = {
    if (section2.reserved != 0)
      ZIO.fail(Section2InvalidReservedByte(section2.reserved))
    else
      ZIO.unit
  }
}