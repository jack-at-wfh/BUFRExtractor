package com.weathernexus.utilities.bufr.readers

import zio.*
import zio.test.*
import zio.test.Assertion.*
import com.weathernexus.utilities.bufr.data.*
import java.io.IOException

object BufrCodeFlagReaderSpec extends ZIOSpecDefault {

  // Manually construct the data map for testing with the correct constructor signature
  val testData: Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]] = Map(
    BufrCodeFlagKey("001003", 0) -> List(
      BufrCodeFlagEntry(
        elementName = "WMO REGION NUMBER/GEOGRAPHICAL AREA",
        entryName = "ANTARCTICA",
        entryNameSub1 = None, 
        entryNameSub2 = None,
        note = None,
        status = None,
        sourceFile = "test_file", // Added missing parameter
        lineNumber = 2             // Added missing parameter
      )
    ),
    BufrCodeFlagKey("001003", 1) -> List(
      BufrCodeFlagEntry(
        elementName = "WMO REGION NUMBER/GEOGRAPHICAL AREA",
        entryName = "REGION I",
        entryNameSub1 = None,
        entryNameSub2 = None,
        note = None,
        status = None,
        sourceFile = "test_file",
        lineNumber = 3
      )
    ),
    BufrCodeFlagKey("001007", 1) -> List(
      BufrCodeFlagEntry(
        elementName = "SATELLITE IDENTIFIER",
        entryName = "ERS 1",
        entryNameSub1 = None,
        entryNameSub2 = None,
        note = None,
        status = None,
        sourceFile = "test_file",
        lineNumber = 4
      )
    ),
    BufrCodeFlagKey("001007", 2) -> List(
      BufrCodeFlagEntry(
        elementName = "SATELLITE IDENTIFIER",
        entryName = "ERS 2",
        entryNameSub1 = None,
        entryNameSub2 = None,
        note = None,
        status = None,
        sourceFile = "test_file",
        lineNumber = 5
      )
    )
  )

  // Create the reader once to be used in all tests
  private val reader = BufrCodeFlagReader(testData)

  val `should get all keys correctly` = test("should get all keys correctly") {
    val allKeys = reader.getAllKeys
    assert(allKeys)(hasSize(equalTo(4))) &&
    assert(allKeys)(contains(BufrCodeFlagKey("001003", 0)))
  }

  val `should correctly get a single entry` = test("should correctly get a single entry") {
    val entry = reader.getFirst(BufrCodeFlagKey("001003", 0))
    assert(entry)(isSome(hasField("entryName", _.entryName, equalTo("ANTARCTICA"))))
  }

  val `should return None for non-existent key` = test("should return None for non-existent key") {
    val entry = reader.getFirst(BufrCodeFlagKey("nonexistent", 99))
    assert(entry)(isNone)
  }

  val `should get all entries for a key` = test("should get all entries for a key") {
    val entries = reader.get(BufrCodeFlagKey("001007", 1))
    assert(entries)(isSome(hasSize(equalTo(1)))) &&
    assert(entries.get.head.entryName)(equalTo("ERS 1"))
  }

  val `should find entries by a predicate` = test("should find entries by a predicate") {
    val matchedEntries = reader.find(_.entryName.contains("REGION"))
    assert(matchedEntries)(hasSize(equalTo(1))) &&
    assert(matchedEntries.head.entryName)(equalTo("REGION I"))
  }

  val `should get the full data map` = test("should get the full data map") {
    val allData = reader.getAll
    assert(allData)(equalTo(testData))
  }

  override def spec: Spec[TestEnvironment, Any] =
    suite("BufrCodeFlagReader")(
      `should get all keys correctly`,
      `should correctly get a single entry`,
      `should return None for non-existent key`,
      `should get all entries for a key`,
      `should find entries by a predicate`,
      `should get the full data map`
    )
}