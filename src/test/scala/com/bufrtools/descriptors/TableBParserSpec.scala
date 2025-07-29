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
        descriptorCode.get.x == 1, 
        descriptorCode.get.y == 1,
        descriptorCode.get.toFXY == "001001"
      )
    }

  val shouldParseMultipleEntriesFromCSVContent = test("should parse multiple entries from CSV content") {
    for {
        entriesMap <- TableBParser.parseTableBContent(testCsv)
    } yield assertTrue(
        entriesMap.size == 3,
        entriesMap.contains(DescriptorCode(0, 1, 1)),
        entriesMap.contains(DescriptorCode(0, 1, 2)),
        entriesMap.contains(DescriptorCode(0, 2, 1))
    )
  }    

  val shouldCreateWorkingTableBService = 
    test("should create working TableBService") {
      for {
        entriesMap <- TableBParser.parseTableBContent(testCsv)
        service = new TableBService(entriesMap)
        wmoBlockEntry = service.getEntry("001001")
        identificationEntries = service.getEntriesByClass("01")
        stats = service.getStatistics
      } yield assertTrue(
        wmoBlockEntry.isDefined,
        wmoBlockEntry.get.elementName == "WMO block number",
        identificationEntries.length == 2,
        stats.totalEntries == 3,
        stats.operationalEntries == 3
      )
    }

  val shouldFailOnMalformedCSVLine = 
    test("should fail on malformed CSV line - too few fields") {
        val malformedLine = "01,Identification,001001,WMO block number"
        
        assertZIO(TableBParser.parseTableBLine(malformedLine).exit)(
            fails(hasMessage(containsString("Expected 14 fields")))
        )
    }
  
  val shouldFailOnInvalidNumericFields = 
    test("should fail on invalid numeric fields") {
        val invalidLine = "01,Identification,001001,WMO block number,Numeric,invalid,0,7,Numeric,0,2,,,Operational"
        
        assertZIO(TableBParser.parseTableBLine(invalidLine).exit)(
            fails(hasMessage(containsString("Invalid scale")))
        )
    }

  val shouldHandleEmptyCSVContent = 
    test("should handle empty CSV content") {
        assertZIO(TableBParser.parseTableBContent("").exit)(
          fails(hasMessage(containsString("Empty CSV content")))
        )
      }

  val shouldFailOnInvalidHeader = 
      test("should fail on invalid header") {
        val badHeader = """BadHeader1,BadHeader2,01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational"""
        
        assertZIO(TableBParser.parseTableBContent(badHeader).exit)(
          fails(hasMessage(containsString("Invalid header")))
        )
      }

  val shouldHandleEmptyOptionalFields =
    test("should handle empty optional fields") {
        val lineWithEmptyFields = "01,Identification,001001,WMO block number,Numeric,,,7,Numeric,,2,,,Operational"
        
        for {
          entry <- TableBParser.parseTableBLine(lineWithEmptyFields)
        } yield assertTrue(
          entry.scale == 0,
          entry.referenceValue == 0L,
          entry.crexScale == 0,
          entry.note.isEmpty,
          entry.noteNumber.isEmpty
        )
    }

  val shouldHandleEntriesWithNotes =
    test("should handle entries with notes") {
        val lineWithNote = "01,Identification,001004,WMO Region sub-area,Numeric,0,0,3,Numeric,0,1,(see Note 9),42,Operational"
        
        for {
          entry <- TableBParser.parseTableBLine(lineWithNote)
        } yield assertTrue(
          entry.note.contains("(see Note 9)"),
          entry.noteNumber.contains(42)
        )
    }
  val shouldHandleQuotedFieldsWithCommas =
    test("should handle quoted fields with commas") {
        val quotedLine = """01,"Identification, extended",001001,"WMO block number, primary",Numeric,0,0,7,Numeric,0,2,,,Operational"""
        
        for {
          entry <- TableBParser.parseTableBLine(quotedLine)
        } yield assertTrue(
          entry.classDescription == "Identification, extended",
          entry.elementName == "WMO block number, primary"
        )
    }

  val shouldIdentifyDescriptorTypesCorrectly =
    test("should identify descriptor types correctly") {
        // Note: In BUFR/CREX, F is actually the first digit, not first two digits
        // The parsing code extracts first 2 chars as F, but logically F should be 0-3
        val testCodes = List(
          ("001001", true, false, false, false),   // Element descriptor (F=0)
          ("101001", false, true, false, false),   // Replication descriptor (F=1) 
          ("201001", false, false, true, false),   // Operator descriptor (F=2)
          ("301001", false, false, false, true)    // Sequence descriptor (F=3)
        )
        
        ZIO.foreach(testCodes) { case (fxyCode, isElement, isReplication, isOperator, isSequence) =>
          ZIO.succeed {
            val desc = DescriptorCode.fromFXY(fxyCode).get
            assertTrue(
              desc.isElementDescriptor == isElement,
              desc.isReplicationDescriptor == isReplication, 
              desc.isOperatorDescriptor == isOperator,
              desc.isSequenceDescriptor == isSequence
            )
          }
        }.map(_.reduce(_ && _))
    }

  val shouldHandleInvalidFXYCodes = 
    test("should handle invalid FXY codes") {
        val invalidCodes = List("", "123", "12345", "1234567", "abcdef", "12345x")
        
        ZIO.succeed {
          assertTrue(
            invalidCodes.forall(code => DescriptorCode.fromFXY(code).isEmpty)
          )
        }
    }

  val shouldSearchEntriesByNamePattern =
    test("should search entries by name pattern") {
      for {
        entriesMap <- TableBParser.parseTableBContent(testCsv)
        service = new TableBService(entriesMap)
        numberEntries = service.searchByName("(?i)number")
          stationEntries = service.searchByName("(?i)station")
          wmoEntries = service.searchByName("(?i)WMO")
        } yield assertTrue(
          numberEntries.length == 2, // WMO block number and WMO station number
          stationEntries.length == 2, // WMO station number and Type of station
          wmoEntries.length == 2      // WMO block number and WMO station number
        )
      }

  val shouldGetElementDescriptorsOnly =
    test("should get element descriptors only") {
        for {
          entriesMap <- TableBParser.parseTableBContent(testCsv)
          service = new TableBService(entriesMap)
          elementDescriptors = service.getElementDescriptors
        } yield assertTrue(
          elementDescriptors.length == 3, // All test entries are element descriptors (F=0)
          elementDescriptors.forall(_.descriptorCode.exists(_.isElementDescriptor))
        )
    }

  val shouldProvideAccurateStatistics =
    test("should provide accurate statistics") {
        for {
          entriesMap <- TableBParser.parseTableBContent(testCsv)
          service = new TableBService(entriesMap)
          stats = service.getStatistics
        } yield assertTrue(
          stats.totalEntries == 3,
          stats.operationalEntries == 3,
          stats.entriesWithNotes == 0,
          stats.classCounts("01") == 2,
          stats.classCounts("02") == 1,
          stats.unitCounts("Numeric") == 2,
          stats.unitCounts("Code table") == 1
        )
    }

  val shouldFailWithInvalidCodeFigure =
    test("should fail with invalid code figure in TableAEntry") {
      for {
        result <- TableAParser.parseTableALine("abc,Surface data - land,Operational").flip
      } yield assertTrue(result match {
        case TableAParseError(msg) => msg.contains("Invalid code figure 'abc'")
        case _ => false
      })
    }

  val shouldIdentifyUnitTypesCorrectly =
    test("should identify unit types correctly") {
      val numericEntry = TableBEntry("01", "Test", "001001", "Test", "Numeric", 0, 0, 8, "Numeric", 0, 3, None, None, "Operational")
      val characterEntry = TableBEntry("01", "Test", "001001", "Test", "CCITT IA5", 0, 0, 64, "Character", 0, 8, None, None, "Operational")
      val codeTableEntry = TableBEntry("01", "Test", "001001", "Test", "Code table", 0, 0, 4, "Code table", 0, 2, None, None, "Operational")
      val flagTableEntry = TableBEntry("01", "Test", "001001", "Test", "Flag table", 0, 0, 8, "Flag table", 0, 3, None, None, "Operational")
        
        assertTrue(
          numericEntry.isNumeric,
          !numericEntry.isCharacter,
          !numericEntry.isCodeTable,
          !numericEntry.isFlagTable,
          characterEntry.isCharacter,
          codeTableEntry.isCodeTable,
          flagTableEntry.isFlagTable
        )
    }


// Grouping tests into suites
  val errorHandlingTestSuite =
    suite("Error handling tests")(
        shouldFailOnMalformedCSVLine,
        shouldFailOnInvalidNumericFields,
        shouldHandleEmptyCSVContent,
        shouldFailOnInvalidHeader   
    )

  val simpleErrorTestSuite = 
    suite("Simple TableBParser tests")(
      shouldParseASimpleCSVLine,
      shouldExtractDescriptorCodeFromFXY,
      shouldParseMultipleEntriesFromCSVContent,
      shouldCreateWorkingTableBService
    )    

  val edgeCaseTestSuite = 
    suite("Edge case handling")(
        shouldHandleEmptyOptionalFields,
        shouldHandleEntriesWithNotes,
        shouldHandleQuotedFieldsWithCommas
    )

  val descriptorCodeFunctionalityTestSuite = 
    suite("DescriptorCode functionality")(
       shouldIdentifyDescriptorTypesCorrectly,
       shouldHandleInvalidFXYCodes
    )

  val tableBServiceAdvancedFunctionalityTestSuite = 
    suite("TableBService advanced functionality")(
      shouldSearchEntriesByNamePattern,
      shouldGetElementDescriptorsOnly,
      shouldProvideAccurateStatistics    
    )

  val dataInterpretationMethodsTestSuite = 
    suite("Data interpretation methods")(
      shouldFailWithInvalidCodeFigure,
      shouldIdentifyUnitTypesCorrectly
    )

// Combine all test suites
  def spec = suite("TableBParserSpec")(
    simpleErrorTestSuite,
    errorHandlingTestSuite,
    edgeCaseTestSuite,
    descriptorCodeFunctionalityTestSuite,
    tableBServiceAdvancedFunctionalityTestSuite,
    dataInterpretationMethodsTestSuite
  )
}