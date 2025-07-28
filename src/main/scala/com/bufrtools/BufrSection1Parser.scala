package com.bufrtools

import zio._
import zio.stream._

// BUFR Section 1 structure (varies by BUFR edition)
case class BufrSection1(
  sectionLength: Int,              // Length of section in bytes (3 bytes)
  masterTableNumber: Int,          // BUFR master table number (1 byte)
  originatingCenterId: Int,        // Originating/generating centre (2 bytes)
  originatingSubcenterId: Int,     // Originating/generating sub-centre (2 bytes)
  updateSequenceNumber: Int,       // Update sequence number (1 byte)
  optionalSection: Boolean,        // Optional section (bit 7 of byte 7)
  dataCategory: Int,               // Data category (1 byte)
  internationalDataSubcategory: Int, // International data sub-category (1 byte)
  localDataSubcategory: Int,       // Local data sub-category (1 byte)
  masterTableVersionNumber: Int,   // Master table version number (1 byte)
  localTableVersionNumber: Int,    // Local table version number (1 byte)
  year: Int,                       // Year (2 bytes, most significant first)
  month: Int,                      // Month (1 byte)
  day: Int,                        // Day (1 byte)
  hour: Int,                       // Hour (1 byte)
  minute: Int,                     // Minute (1 byte)
  second: Option[Int]              // Second (1 byte, only in edition 4+)
)

// Custom errors for Section 1 parsing
sealed trait BufrSection1ParseError extends Exception
case class Section1InsufficientData(expected: Int, actual: Int) extends BufrSection1ParseError {
  override def getMessage: String = s"Section 1: Expected $expected bytes, got $actual"
}
case class Section1InvalidLength(length: Int, minExpected: Int) extends BufrSection1ParseError {
  override def getMessage: String = s"Section 1: Invalid length $length, minimum expected $minExpected"
}
case class Section1InvalidDate(year: Int, month: Int, day: Int) extends BufrSection1ParseError {
  override def getMessage: String = s"Section 1: Invalid date $year-$month-$day"
}
case class Section1InvalidTime(hour: Int, minute: Int, second: Option[Int]) extends BufrSection1ParseError {
  override def getMessage: String = s"Section 1: Invalid time $hour:$minute${second.map(s => s":$s").getOrElse("")}"
}

object BufrSection1Parser {
  
  // Parse Section 1 from a Chunk[Byte], requires BUFR edition for proper parsing
  def parseSection1(data: Chunk[Byte], bufrEdition: Int): IO[BufrSection1ParseError, BufrSection1] = {
    for {
      _              <- validateMinimumLength(data, bufrEdition)
      sectionLength  <- extractSectionLength(data)
      _              <- validateSectionLength(sectionLength, data.length, bufrEdition)
      section1       <- extractSection1Fields(data, bufrEdition, sectionLength)
      _              <- validateDateTime(section1)
    } yield section1
  }
  
  // Create a ZIO Stream transformer
  def parseSection1Stream(bufrEdition: Int): ZPipeline[Any, BufrSection1ParseError, Chunk[Byte], BufrSection1] =
    ZPipeline.mapZIO(parseSection1(_, bufrEdition))
  
  private def validateMinimumLength(data: Chunk[Byte], bufrEdition: Int): IO[BufrSection1ParseError, Unit] = {
    val minLength = if (bufrEdition >= 4) 22 else 21 // Edition 4+ has second field
    if (data.length < minLength)
      ZIO.fail(Section1InsufficientData(minLength, data.length))
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
  
  private def validateSectionLength(sectionLength: Int, dataLength: Int, bufrEdition: Int): IO[BufrSection1ParseError, Unit] = {
    val minLength = if (bufrEdition >= 4) 22 else 21
    if (sectionLength < minLength)
      ZIO.fail(Section1InvalidLength(sectionLength, minLength))
    else if (sectionLength > dataLength)
      ZIO.fail(Section1InsufficientData(sectionLength, dataLength))
    else
      ZIO.unit
  }
  
  private def extractSection1Fields(data: Chunk[Byte], bufrEdition: Int, sectionLength: Int): IO[Nothing, BufrSection1] =
    ZIO.succeed {
      val bytes = data.toArray
      
      val masterTableNumber = bytes(3) & 0xFF
      val originatingCenterId = ((bytes(4) & 0xFF) << 8) | (bytes(5) & 0xFF)
      val originatingSubcenterId = ((bytes(6) & 0xFF) << 8) | (bytes(7) & 0xFF)
      val updateSequenceNumber = bytes(8) & 0xFF
      val optionalSection = (bytes(9) & 0x80) != 0 // Check bit 7
      val dataCategory = bytes(10) & 0xFF
      val internationalDataSubcategory = bytes(11) & 0xFF
      val localDataSubcategory = bytes(12) & 0xFF
      val masterTableVersionNumber = bytes(13) & 0xFF
      val localTableVersionNumber = bytes(14) & 0xFF
      val year = ((bytes(15) & 0xFF) << 8) | (bytes(16) & 0xFF)
      val month = bytes(17) & 0xFF
      val day = bytes(18) & 0xFF
      val hour = bytes(19) & 0xFF
      val minute = bytes(20) & 0xFF
      val second = if (bufrEdition >= 4 && sectionLength > 21) Some(bytes(21) & 0xFF) else None
      
      BufrSection1(
        sectionLength = sectionLength,
        masterTableNumber = masterTableNumber,
        originatingCenterId = originatingCenterId,
        originatingSubcenterId = originatingSubcenterId,
        updateSequenceNumber = updateSequenceNumber,
        optionalSection = optionalSection,
        dataCategory = dataCategory,
        internationalDataSubcategory = internationalDataSubcategory,
        localDataSubcategory = localDataSubcategory,
        masterTableVersionNumber = masterTableVersionNumber,
        localTableVersionNumber = localTableVersionNumber,
        year = year,
        month = month,
        day = day,
        hour = hour,
        minute = minute,
        second = second
      )
    }
  
  private def validateDateTime(section1: BufrSection1): IO[BufrSection1ParseError, Unit] = {
    val validDate = section1.month >= 1 && section1.month <= 12 &&
                   section1.day >= 1 && section1.day <= 31
    val validTime = section1.hour <= 23 && section1.minute <= 59 &&
                   section1.second.forall(_ <= 59)
    
    if (!validDate)
      ZIO.fail(Section1InvalidDate(section1.year, section1.month, section1.day))
    else if (!validTime)
      ZIO.fail(Section1InvalidTime(section1.hour, section1.minute, section1.second))
    else
      ZIO.unit
  }
}