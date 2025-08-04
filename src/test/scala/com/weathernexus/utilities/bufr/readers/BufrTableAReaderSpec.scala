package com.weathernexus.utilities.bufr.readers

import zio.*
import zio.test.*
import zio.test.Assertion.*
import com.weathernexus.utilities.bufr.data.*
import java.io.IOException

object BufrTableAReaderSpec extends ZIOSpecDefault {

  // Manually construct the data map for testing
  val testData: Map[BufrTableAKey, List[BufrTableAEntry]] = Map(
    BufrTableAKey(0) -> List(
      BufrTableAEntry(
        meaning = "Surface data - land",
        status = Some("Operational"),
        sourceFile = "test_file",
        lineNumber = 2
      )
    ),
    BufrTableAKey(1) -> List(
      BufrTableAEntry(
        meaning = "Surface data - sea",
        status = Some("Operational"),
        sourceFile = "test_file",
        lineNumber = 3
      )
    ),
    BufrTableAKey(11) -> List(
      BufrTableAEntry(
        meaning = "BUFR tables, complete replacement or update",
        status = Some("Operational"),
        sourceFile = "test_file",
        lineNumber = 4
      )
    ),
    BufrTableAKey(15) -> List(
      BufrTableAEntry(
        meaning = "Reserved",
        status = Some("Operational"),
        sourceFile = "test_file",
        lineNumber = 5
      )
    )
  )

  // Create the reader once to be used in all tests
  private val reader = BufrTableAReader(testData)

  val `should get all keys correctly` = test("should get all keys correctly") {
    val allKeys = reader.getAllKeys
    assert(allKeys)(hasSize(equalTo(4))) &&
    assert(allKeys)(contains(BufrTableAKey(0)))
  }

  val `should correctly get a single entry` = test("should correctly get a single entry") {
    val entry = reader.getFirst(BufrTableAKey(0))
    assert(entry)(isSome(hasField("meaning", _.meaning, equalTo("Surface data - land"))))
  }

  val `should return None for non-existent key` = test("should return None for non-existent key") {
    val entry = reader.getFirst(BufrTableAKey(99))
    assert(entry)(isNone)
  }

  val `should get all entries for a key` = test("should get all entries for a key") {
    // Assuming a key can have multiple entries in a theoretical case, though Table A typically has one per key
    // For this test, we can use a key that has a single entry for validation
    val entries = reader.get(BufrTableAKey(11))
    assert(entries)(isSome(hasSize(equalTo(1)))) &&
    assert(entries.get.head.meaning)(equalTo("BUFR tables, complete replacement or update"))
  }

  val `should find entries by a predicate` = test("should find entries by a predicate") {
    val matchedEntries = reader.find(_.meaning.contains("data"))
    assert(matchedEntries)(hasSize(equalTo(2))) &&
    assert(matchedEntries.map(_.meaning).toSet)(equalTo(Set("Surface data - land", "Surface data - sea")))
  }

  val `should get the full data map` = test("should get the full data map") {
    val allData = reader.getAll
    assert(allData)(equalTo(testData))
  }

  override def spec: Spec[TestEnvironment, Any] =
    suite("BufrTableAReaderSpec")(
      `should get all keys correctly`,
      `should correctly get a single entry`,
      `should return None for non-existent key`,
      `should get all entries for a key`,
      `should find entries by a predicate`,
      `should get the full data map`
    )
}