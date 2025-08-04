package com.weathernexus.utilities.bufr.readers

import zio.test.*
import zio.test.Assertion.*
import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.bufr.descriptors.DescriptorCode
import java.io.IOException

object BufrTableBReaderSpec extends ZIOSpecDefault {

  // Manually construct the data map for testing, using BufrTableBKey as the key type
  val testData: Map[BufrTableBKey, List[BufrTableBEntry]] = Map(
    BufrTableBKey("001001") -> List(
      BufrTableBEntry(
        classNumber = 0,
        className = "Identification",
        elementName = "WMO block number",
        note = None,
        bufrUnit = "Numeric",
        bufrScale = 0,
        bufrReferenceValue = 0,
        bufrDataWidth = 7,
        crexUnit = None,
        crexScale = None,
        crexDataWidth = None,
        status = Some("Operational"),
        sourceFile = "test_file.csv",
        lineNumber = 2
      )
    ),
    BufrTableBKey("001002") -> List(
      BufrTableBEntry(
        classNumber = 0,
        className = "Identification",
        elementName = "WMO station number",
        note = None,
        bufrUnit = "Numeric",
        bufrScale = 0,
        bufrReferenceValue = 0,
        bufrDataWidth = 10,
        crexUnit = None,
        crexScale = None,
        crexDataWidth = None,
        status = Some("Operational"),
        sourceFile = "test_file.csv",
        lineNumber = 3
      )
    ),
    BufrTableBKey("002001") -> List(
      BufrTableBEntry(
        classNumber = 0,
        className = "Instrumentation",
        elementName = "Type of station",
        note = Some("Some note"),
        bufrUnit = "Code table",
        bufrScale = 0,
        bufrReferenceValue = 0,
        bufrDataWidth = 2,
        crexUnit = None,
        crexScale = None,
        crexDataWidth = None,
        status = Some("Operational"),
        sourceFile = "test_file.csv",
        lineNumber = 4
      )
    ),
    BufrTableBKey("025055") -> List(
      BufrTableBEntry(
        classNumber = 0,
        className = "Vertical coordinate",
        elementName = "Pressure",
        note = None,
        bufrUnit = "Pa",
        bufrScale = -1,
        bufrReferenceValue = 0,
        bufrDataWidth = 16,
        crexUnit = None,
        crexScale = None,
        crexDataWidth = None,
        status = Some("Operational"),
        sourceFile = "test_file.csv",
        lineNumber = 5
      )
    )
  )

  // Create the reader once to be used in all tests
  private val reader = BufrTableBReader(testData)

  val `should get all keys correctly` = test("should get all keys correctly") {
    val allKeys = reader.getAllKeys
    assert(allKeys)(hasSize(equalTo(4))) &&
    assert(allKeys)(contains(BufrTableBKey("001001")))
  }

  val `should correctly get a single entry` = test("should correctly get a single entry") {
    val entry = reader.getFirst(BufrTableBKey("001001"))
    assert(entry)(isSome(hasField("elementName", (_: BufrTableBEntry).elementName, equalTo("WMO block number"))))
  }

  val `should return None for non-existent key` = test("should return None for non-existent key") {
    val entry = reader.getFirst(BufrTableBKey("999999"))
    assert(entry)(isNone)
  }

  val `should get all entries for a key` = test("should get all entries for a key") {
    val entries = reader.get(BufrTableBKey("002001"))
    assert(entries)(isSome(hasSize(equalTo(1)))) &&
    assert(entries.get.head.note)(isSome(equalTo("Some note")))
  }

  val `should find entries by a predicate` = test("should find entries by a predicate") {
    val matchedEntries = reader.find(_.bufrUnit == "Numeric")
    assert(matchedEntries)(hasSize(equalTo(2))) &&
    assert(matchedEntries.map(_.elementName).toSet)(equalTo(Set("WMO block number", "WMO station number")))
  }

  val `should get the full data map` = test("should get the full data map") {
    val allData = reader.getAll
    assert(allData)(equalTo(testData))
  }

  
  val `fromEntry should create a key from a BufrTableB instance` = test("fromEntry should create a key from a BufrTableB instance") {
      val entry = BufrTableB(
        classNumber = 1,
        className = "Identification",
        fxy = "001001",
        elementName = "WMO block number",
        note = None,
        bufrUnit = "Numeric",
        bufrScale = 0,
        bufrReferenceValue = 0,
        bufrDataWidth = 7,
        crexUnit = None,
        crexScale = None,
        crexDataWidth = None,
        status = Some("Operational"),
        sourceFile = "test_file.csv",
        lineNumber = 1
      )
      val key = BufrTableBKey.fromEntry(entry)
      assert(key)(equalTo(BufrTableBKey("001001")))
    }

  val `fromString should successfully parse a valid 6-digit string` = test("fromString should successfully parse a valid 6-digit string") {
      val keyString = "002001"
      val parsedKey = BufrTableBKey.fromString(keyString)
      assert(parsedKey)(isSome(equalTo(BufrTableBKey(keyString))))
    }

  val `fromString should return None for a string that is too short` = test("fromString should return None for a string that is too short") {
      val keyString = "123"
      val parsedKey = BufrTableBKey.fromString(keyString)
      assert(parsedKey)(isNone)
    }

  val `fromString should return None for a string that is too long` = test("fromString should return None for a string that is too long") {
      val keyString = "0010010"
      val parsedKey = BufrTableBKey.fromString(keyString)
      assert(parsedKey)(isNone)
    }

  override def spec: Spec[TestEnvironment, Any] =
    suite("BufrTableBReaderSpec")(
      `should get all keys correctly`,
      `should correctly get a single entry`,
      `should return None for non-existent key`,
      `should get all entries for a key`,
      `should find entries by a predicate`,
      `should get the full data map`,
      `fromEntry should create a key from a BufrTableB instance`,
      `fromString should successfully parse a valid 6-digit string`,
      `fromString should return None for a string that is too short`,
      `fromString should return None for a string that is too long` 
    )
}