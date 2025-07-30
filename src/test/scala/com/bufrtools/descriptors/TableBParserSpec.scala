package com.bufrtools.descriptors

import zio._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import com.bufrtools.csv.*

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

  val shouldFailOnEmptyInputLine = 
    test("should fail on empty input line") {
      val emptyLine = ""
      
      assertZIO(TableBParser.parseTableBLine(emptyLine).exit)(
        fails(hasMessage(containsString("Expected 14 fields")))
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

  val shouldEncodeAndDecodeNumericValuesCorrectly =
  test("should encode and decode numeric values correctly") {
    val entry = TableBEntry(
      classNumber = "12", classDescription = "Temperature", 
      fxyCode = "012001", elementName = "Temperature/dry-bulb temperature",
      unit = "K", scale = 2, referenceValue = 0L, dataWidth = 12,
      crexUnit = "K", crexScale = 2, crexDataWidth = 5,
      note = None, noteNumber = None, status = "Operational"
    )
    
    val actualTemp = 273.15  // 0°C in Kelvin
    val encoded = entry.encodeValue(actualTemp)
    val decoded = entry.actualValue(encoded)
    
    assertTrue(
      encoded == 27315L,  // 273.15 * 100
      math.abs(decoded - actualTemp) < 0.01 // Should be very close
    )
  }  

// Tests for resource and file loading functions

  val shouldFailOnNonExistentResource = 
    test("should fail on non-existent resource") {
      val nonExistentPath = "/non-existent-resource.csv"
      
      assertZIO(TableBParser.loadTableBFromResource(nonExistentPath).exit)(
        fails(hasMessage(containsString("Resource not found")))
      )
    }

  val shouldFailOnNonExistentFile = 
    test("should fail on non-existent file") {
      val nonExistentFile = "/path/to/non-existent-file.csv"
      
      assertZIO(TableBParser.loadTableBFromFile(nonExistentFile).exit)(
        fails(hasMessage(containsString("Failed to load file")))
      )
    }

  val shouldUseDefaultResourcePath = 
    test("should use default resource path when none provided") {
      // Test that it attempts to use the default path (will likely fail but with correct error message)
      assertZIO(TableBParser.loadTableBFromResource().exit)(
        fails(hasMessage(containsString("/bufr-tables/table-B-BUFRCREX.csv"))) ||
        succeeds(anything) // If the resource actually exists, that's fine too
      )
    }

  val shouldLoadFromTemporaryFile = 
    test("should load from temporary file") {
      val tempContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
  01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational
  01,Identification,001002,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Operational"""
      
      for {
        // Create temporary file
        tempFile <- ZIO.attemptBlocking {
          val file = java.io.File.createTempFile("table-b-test", ".csv")
          file.deleteOnExit()
          val writer = new java.io.FileWriter(file)
          writer.write(tempContent)
          writer.close()
          file.getAbsolutePath
        }
        
        // Load from temporary file
        entries <- TableBParser.loadTableBFromFile(tempFile)
        
      } yield assertTrue(
        entries.size == 2,
        entries.contains(DescriptorCode(0, 1, 1)),
        entries.contains(DescriptorCode(0, 1, 2))
      )
    }

  val shouldHandleInvalidFilePermissions = 
    test("should handle invalid file permissions") {
      // Try to read from a directory instead of a file (should fail)
      val directoryPath = java.lang.System.getProperty("java.io.tmpdir")
      
      assertZIO(TableBParser.loadTableBFromFile(directoryPath).exit)(
        fails(hasMessage(containsString("Failed to load file")))
      )
    }

  val shouldHandleInvalidScaleField = 
    test("should handle invalid scale field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,invalid_scale,0,7,Numeric,0,2,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid scale")))
      )
    }

  val shouldHandleInvalidReferenceValueField = 
    test("should handle invalid reference value field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,invalid_ref,7,Numeric,0,2,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid reference value")))
      )
    }

  val shouldHandleInvalidDataWidthField = 
    test("should handle invalid data width field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,invalid_width,Numeric,0,2,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid data width")))
      )
    }

  val shouldHandleInvalidCrexScaleField = 
    test("should handle invalid CREX scale field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,invalid_crex_scale,2,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid CREX scale")))
      )
    }

  val shouldHandleInvalidCrexDataWidthField = 
    test("should handle invalid CREX data width field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,invalid_crex_width,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid CREX data width")))
      )
    }

  val shouldHandleInvalidNoteNumberField = 
    test("should handle invalid note number field") {
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,invalid_note,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(hasMessage(containsString("Invalid note number")))
      )
    }

  val shouldHandleCorruptedResourceContent = 
    test("should handle corrupted resource content - general test") {
      // Test with multiple error conditions to ensure error propagation works
      val corruptedContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,invalid,0,7,Numeric,0,2,,,Operational"""
      
      assertZIO(TableBParser.parseTableBContent(corruptedContent).exit)(
        fails(isSubtype[TableBParseError](anything))
      )
    }

  val shouldUseStreamBasedParser =
    test("should use stream-based parser") {
      val csvLines = List(
        "01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational",
        "01,Identification,001002,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Operational"
      )
      
      for {
        entries <- ZStream.fromIterable(csvLines)
          .via(TableBParser.parseTableBStream)
          .runCollect
      } yield assertTrue(
        entries.length == 2,
        entries(0).fxyCode == "001001",
        entries(1).fxyCode == "001002"
      )
    }

  val shouldHandleStreamParsingErrors =
    test("should handle stream parsing errors") {
      val csvLines = List(
        "01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational",
        "01,Identification,001002,WMO station number,Numeric,invalid,0,10,Numeric,0,3,,,Operational"
      )
      
      assertZIO(
        ZStream.fromIterable(csvLines)
          .via(TableBParser.parseTableBStream)
          .runCollect
          .exit
      )(
        fails(isSubtype[TableBParseError](anything))
      )
    }

  val shouldParseFromStringMethod =
    test("should parse from string method") {
      for {
        entriesMap <- TableBParser.parseFromString(testCsv)
      } yield assertTrue(
        entriesMap.size == 3,
        entriesMap.contains(DescriptorCode(0, 1, 1)),
        entriesMap.contains(DescriptorCode(0, 1, 2)),
        entriesMap.contains(DescriptorCode(0, 2, 1))
      )
    }

  val shouldGetEntryByDescriptorCode =
    test("should get entry by descriptor code") {
      for {
        entriesMap <- TableBParser.parseTableBContent(testCsv)
        service = new TableBService(entriesMap)
        entry = service.getEntry(DescriptorCode(0, 1, 1))
        nonExistentEntry = service.getEntry(DescriptorCode(9, 9, 9))
      } yield assertTrue(
        entry.isDefined,
        entry.get.fxyCode == "001001",
        entry.get.elementName == "WMO block number",
        nonExistentEntry.isEmpty
      )
    }

  val shouldGetOperationalEntriesOnly =
    test("should get operational entries only") {
      // Create test data with mix of operational and non-operational entries
      val mixedStatusCsv = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,001001,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational
01,Identification,001002,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Deprecated
02,Instrumentation,002001,Type of station,Code table,0,0,2,Code table,0,1,,,Operational
02,Instrumentation,002002,Test equipment,Code table,0,0,4,Code table,0,2,,,Experimental"""

      for {
        entriesMap <- TableBParser.parseTableBContent(mixedStatusCsv)
        service = new TableBService(entriesMap)
        operationalEntries = service.getOperationalEntries
      } yield assertTrue(
        operationalEntries.length == 2, // Only operational entries
        operationalEntries.forall(_.isOperational),
        operationalEntries.exists(_.fxyCode == "001001"),
        operationalEntries.exists(_.fxyCode == "002001"),
        !operationalEntries.exists(_.fxyCode == "001002"), // Deprecated should be excluded
        !operationalEntries.exists(_.fxyCode == "002002")  // Experimental should be excluded
      )
    }

  val shouldHandleEmptyDataWidthField =
    test("should handle empty data width field - defaults to 0") {
      // Test line 196: if (fields(7).isEmpty) 0 else fields(7).toInt
      // Field 7 (BUFR_DataWidth_Bits) is empty, should default to 0
      val lineWithEmptyDataWidth = "01,Identification,001001,WMO block number,Numeric,0,0,,Numeric,0,2,,,Operational"
      
      for {
        entry <- TableBParser.parseTableBLine(lineWithEmptyDataWidth)
      } yield assertTrue(
        entry.dataWidth == 0, // Should default to 0 when field is empty
        entry.classNumber == "01",
        entry.fxyCode == "001001"
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

  val simpleParserTestSuite = 
    suite("Simple TableBParser tests")(
      shouldParseASimpleCSVLine,
      shouldFailOnEmptyInputLine,
      shouldExtractDescriptorCodeFromFXY,
      shouldParseMultipleEntriesFromCSVContent,
      shouldCreateWorkingTableBService
    )    

  val edgeCaseTestSuite = 
    suite("Edge case handling")(
        shouldHandleEmptyOptionalFields,
        shouldHandleEntriesWithNotes,
        shouldHandleQuotedFieldsWithCommas,
        shouldHandleEmptyDataWidthField
    )

  val tableBServiceAdvancedFunctionalityTestSuite = 
    suite("TableBService advanced functionality")(
      shouldSearchEntriesByNamePattern,
      shouldGetElementDescriptorsOnly,
      shouldProvideAccurateStatistics    
    )

  val dataInterpretationMethodsTestSuite = 
    suite("Data interpretation methods")(
      shouldIdentifyUnitTypesCorrectly,
      shouldEncodeAndDecodeNumericValuesCorrectly
    )

  val resourceAndFileLoadingTestSuite = 
    suite("Resource and file loading tests")(
      shouldFailOnNonExistentResource,
      shouldFailOnNonExistentFile,
      shouldUseDefaultResourcePath,
      shouldLoadFromTemporaryFile,
      shouldHandleInvalidFilePermissions,
      shouldHandleInvalidScaleField,
      shouldHandleInvalidReferenceValueField,
      shouldHandleInvalidDataWidthField,
      shouldHandleInvalidCrexScaleField,
      shouldHandleInvalidCrexDataWidthField,
      shouldHandleInvalidNoteNumberField,
      shouldHandleCorruptedResourceContent
    )

  val alternativeParsingMethodsTestSuite = 
    suite("Alternative parsing methods")(
      shouldUseStreamBasedParser,
      shouldHandleStreamParsingErrors,
      shouldParseFromStringMethod
    )

  val additionalTableBServiceTestSuite = 
    suite("Additional TableBService functionality")(
      shouldGetEntryByDescriptorCode,
      shouldGetOperationalEntriesOnly      
    )

// Combine all test suites
  def spec = suite("TableBParserSpec")(
    simpleParserTestSuite,
    errorHandlingTestSuite,
    edgeCaseTestSuite,
    tableBServiceAdvancedFunctionalityTestSuite,
    dataInterpretationMethodsTestSuite,
    resourceAndFileLoadingTestSuite,
    alternativeParsingMethodsTestSuite,
    additionalTableBServiceTestSuite
  )
}