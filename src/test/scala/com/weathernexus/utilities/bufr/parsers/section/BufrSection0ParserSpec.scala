package com.weathernexus.utilities.bufr.parsers.section

import zio._
import zio.stream._ 
import zio.test._

object BufrSection0ParserSpec extends ZIOSpecDefault {
  
  val shouldParseValidBUFRSection0 =
    test("should parse valid BUFR Section 0") {
      val validBufrData: Chunk[Byte] = Chunk.fromArray(
        "BUFR".getBytes("ASCII") ++         // Identifier
        Array(0x00.toByte, 0x00.toByte, 0x20.toByte) ++   // Length: 32 bytes  
        Array(0x04.toByte)                  // Edition: 4
      )
      
      for {
        result <- BufrSection0Parser.parseSection0(validBufrData)
      } yield assertTrue(
        result.identifier == "BUFR" &&
        result.totalLength == 32 &&
        result.bufrEdition == 4
      )
    }

  val shouldFailWithInvalidIdentifier =
    test("should fail with InvalidIdentifier for wrong identifier") {
        val invalidIdentifierData = Chunk.fromArray(
            "GRIB".getBytes("ASCII") ++              // Wrong identifier
            Array(0x00.toByte, 0x00.toByte, 0x20.toByte) ++
            Array(0x04.toByte)
        )
    
        for {
            result <- BufrSection0Parser.parseSection0(invalidIdentifierData).flip
        } yield assertTrue(result match {
            case InvalidIdentifier("GRIB") => true
            case _ => false
        })
    }

  val shouldFailWithInsufficientData =    
    test("should fail with InsufficientData for short data") {
        val shortData = Chunk.fromArray(
            "BUFR".getBytes("ASCII") ++              // Only 4 bytes, need 8
            Array(0x00.toByte, 0x00.toByte)          // Missing length and edition
        )
        
        for {
            result <- BufrSection0Parser.parseSection0(shortData).flip
        } yield assertTrue(result match {
            case InsufficientData(8, 6) => true
            case _ => false
        })
    }

  val shouldFailWithInvalidBufrEdition =    
    test("should fail with InvalidBufrEdition for unsupported edition") {
        val invalidEditionData = Chunk.fromArray(
            "BUFR".getBytes("ASCII") ++
            Array(0x00.toByte, 0x00.toByte, 0x20.toByte) ++
            Array(0x01.toByte)                       // Edition 1 (unsupported)
        )
        
        for {
            result <- BufrSection0Parser.parseSection0(invalidEditionData).flip
        } yield assertTrue(result match {
            case InvalidBufrEdition(1) => true
            case _ => false
        })
    }

  val invalidIdentifierShouldFormatErrorMessage =
    test("InvalidIdentifier should format error message correctly") {
      val error = InvalidIdentifier("GRIB")
      assertTrue(error.getMessage == "Expected 'BUFR' identifier, found: 'GRIB'")
    }

  val insufficientDataShouldFormatErrorMessage =  
    test("InsufficientData should format error message correctly") {
      val error = InsufficientData(8, 6)
      assertTrue(error.getMessage == "Expected 8 bytes, got 6")
    }
    
  val invalidBufrEditionShouldFormatErrorMessage =      
    test("InvalidBufrEdition should format error message correctly") {
      val error = InvalidBufrEdition(1)
      assertTrue(error.getMessage == "Unsupported BUFR edition: 1")
    }

  val parseSection0StreamShouldProcessMultipleBufrMessages =
    test("parseSection0Stream should process multiple BUFR messages") {
      val validBufrData1 = Chunk.fromArray(
        "BUFR".getBytes("ASCII") ++
        Array(0x00.toByte, 0x00.toByte, 0x20.toByte) ++
        Array(0x04.toByte)
      )
      
      val validBufrData2 = Chunk.fromArray(
        "BUFR".getBytes("ASCII") ++
        Array(0x00.toByte, 0x01.toByte, 0x00.toByte) ++  // Length: 256
        Array(0x03.toByte)                               // Edition: 3
      )
      
      val inputStream = ZStream(validBufrData1, validBufrData2)
      
      for {
        results <- inputStream
          .via(BufrSection0Parser.parseSection0Stream)
          .runCollect
      } yield assertTrue(
        results.size == 2 &&
        results(0).identifier == "BUFR" &&
        results(0).totalLength == 32 &&
        results(0).bufrEdition == 4 &&
        results(1).identifier == "BUFR" &&
        results(1).totalLength == 256 &&
        results(1).bufrEdition == 3
      )
    }

  def spec = suite("BufrSection0Parser")(
    shouldParseValidBUFRSection0,
    shouldFailWithInvalidIdentifier,
    shouldFailWithInsufficientData,
    shouldFailWithInvalidBufrEdition,
    invalidIdentifierShouldFormatErrorMessage,
    insufficientDataShouldFormatErrorMessage,
    invalidBufrEditionShouldFormatErrorMessage,
    parseSection0StreamShouldProcessMultipleBufrMessages
  )
}