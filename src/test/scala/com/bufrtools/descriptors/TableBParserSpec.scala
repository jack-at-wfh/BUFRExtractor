package com.bufrtools.descriptors

import zio._
import zio.test._
import zio.test.Assertion._

object TableBParserSpec extends ZIOSpecDefault {

  // Simple test data with just a few entries
  val testCsv = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational
01,Identification,001002,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Operational
02,Instrumentation,002001,Type of station,Code table,0,0,2,Code table,0,1,,,Operational"""

  val line = "01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational"

  val shouldParseASimpleCSVLine = 
        test("should parse a simple CSV line") {    
      for {
        entry <- TableBParser.parseTableBLine(line)
      } yield assertTrue(
        entry.classNumber == "01",
        entry.classDescription == "Identification", 
        entry.fxyCode == "001001",
        entry.elementName == "WMO block number",
        entry.unit == "Numeric",
        entry.scale == 0,
        entry.referenceValue == 0L,
        entry.dataWidth == 7,
        entry.status == "Operational",
        entry.isOperational == true
      )
    }

  val shouldExtractDescriptorCodeFromFXY = 
    test("should extract descriptor code from FXY") {  
      for {
        entry <- TableBParser.parseTableBLine(line)
        descriptorCode = entry.descriptorCode
      } yield assertTrue(
        descriptorCode.isDefined,
        descriptorCode.get.f == 0,
        descriptorCode.get.x == 10, 
        descriptorCode.get.y == 1,
        descriptorCode.get.toFXY == "001001"
      )
    }

  val shouldParseMultipleEntriesFromCSVContent = test("should parse multiple entries from CSV content") {
    for {
        entriesMap <- TableBParser.parseTableBContent(testCsv)
    } yield assertTrue(
        entriesMap.size == 3,
        entriesMap.contains(DescriptorCode(0, 10, 1)),
        entriesMap.contains(DescriptorCode(0, 10, 2)),
        entriesMap.contains(DescriptorCode(0, 20, 1))
    )
  }    

  def spec = suite("TableBParserSpec")(
    shouldParseASimpleCSVLine,
    shouldExtractDescriptorCodeFromFXY,
    shouldParseMultipleEntriesFromCSVContent
  )
}

    // Additional test to ensure all entries are operational    
    // test("should create working TableBService") {
    //   for {
    //     entriesMap <- TableBParser.parseTableBContent(testCsv)
    //     service = new TableBService(entriesMap)
    //     wmoBlockEntry = service.getEntry("001001")
    //     identificationEntries = service.getEntriesByClass("01")
    //     stats = service.getStatistics
    //   } yield assertTrue(
    //     wmoBlockEntry.isDefined,
    //     wmoBlockEntry.get.elementName == "WMO block number",
    //     identificationEntries.length == 2,
    //     stats.totalEntries == 3,
    //     stats.operationalEntries == 3
    //   )
    // }