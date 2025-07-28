package com.bufrtools

import zio._
import zio.stream._

// BUFR Section 0 structure
case class BufrSection0(
  identifier: String,        // "BUFR" (4 bytes)
  totalLength: Int,         // Total message length in bytes (3 bytes)
  bufrEdition: Int          // BUFR edition number (1 byte)
)

// Custom errors for BUFR parsing
sealed trait BufrParseError extends Exception
case class InvalidIdentifier(found: String) extends BufrParseError {
  override def getMessage: String = s"Expected 'BUFR' identifier, found: '$found'"
}
case class InsufficientData(expected: Int, actual: Int) extends BufrParseError {
  override def getMessage: String = s"Expected $expected bytes, got $actual"
}
case class InvalidBufrEdition(edition: Int) extends BufrParseError {
  override def getMessage: String = s"Unsupported BUFR edition: $edition"
}

object BufrSection0Parser {
  
  // Parse a single Section 0 from a Chunk[Byte]
  def parseSection0(data: Chunk[Byte]): IO[BufrParseError, BufrSection0] = {
    for {
      _          <- validateLength(data)
      identifier <- extractIdentifier(data)
      _          <- validateIdentifier(identifier)
      length     <- extractTotalLength(data)
      edition    <- extractBufrEdition(data)
      _          <- validateBufrEdition(edition)
    } yield BufrSection0(identifier, length, edition)
  }
  
  // Create a ZIO Stream transformer
  def parseSection0Stream: ZPipeline[Any, BufrParseError, Chunk[Byte], BufrSection0] =
    ZPipeline.mapZIO(parseSection0)
  
  private def validateLength(data: Chunk[Byte]): IO[BufrParseError, Unit] =
    if (data.length < 8) 
      ZIO.fail(InsufficientData(8, data.length))
    else 
      ZIO.unit
  
  private def extractIdentifier(data: Chunk[Byte]): IO[Nothing, String] =
    ZIO.succeed(new String(data.take(4).toArray, "ASCII"))
  
  private def validateIdentifier(identifier: String): IO[BufrParseError, Unit] =
    if (identifier == "BUFR")
      ZIO.unit
    else
      ZIO.fail(InvalidIdentifier(identifier))
  
  private def extractTotalLength(data: Chunk[Byte]): IO[Nothing, Int] =
    ZIO.succeed {
      val bytes = data.slice(4, 7).toArray
      // Convert 3 bytes to int (big-endian)
      ((bytes(0) & 0xFF) << 16) | 
      ((bytes(1) & 0xFF) << 8) | 
      (bytes(2) & 0xFF)
    }
  
  private def extractBufrEdition(data: Chunk[Byte]): IO[Nothing, Int] =
    ZIO.succeed(data(7) & 0xFF)
  
  private def validateBufrEdition(edition: Int): IO[BufrParseError, Unit] =
    if (edition >= 2 && edition <= 4) // Common BUFR editions
      ZIO.unit
    else
      ZIO.fail(InvalidBufrEdition(edition))
}