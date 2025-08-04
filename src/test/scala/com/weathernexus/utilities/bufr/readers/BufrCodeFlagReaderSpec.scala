package com.weathernexus.utilities.bufr.readers

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.stream.*
import zio.nio.file.*
import com.weathernexus.utilities.common.io.*
import com.weathernexus.utilities.bufr.parsers.*
import com.weathernexus.utilities.bufr.aggregators.*
import com.weathernexus.utilities.bufr.data.*

object BufrCodeFlagReaderSpec extends ZIOSpecDefault {

  val sampleData =
    """FXY,ElementName_en,CodeFigure,EntryName_en,EntryName_sub1_en,EntryName_sub2_en,Note_en,Status
      |001003,WMO REGION NUMBER/GEOGRAPHICAL AREA,0,ATARCTICA,,,,
      |001003,WMO REGION NUMBER/GEOGRAPHICAL AREA,1,REGION I,,,,
      |001007,SATELLITE IDENTIFIER,1,ERS 1,,,,
      |001007,SATELLITE IDENTIFIER,2,ERS 2,,,,
      |""".stripMargin

  def spec = suite("BufrCodeFlagReader")(
    test("should correctly aggregate data and allow retrieval") {
      for {
        // 1. Setup
        tempFile <- ZIO.succeed(Path("temp_bufr_code_flag.txt"))
        _ <- Files.writeBytes(tempFile, Chunk.fromArray(sampleData.getBytes()))
        
        // 2. Integration Pipeline
        stream = FileNameStream(List(tempFile.toString)) >>> FileToRowsPipeline() >>> ZPipeline.drop(1) >>> BufrParserPipeline()
          
        aggregatedMap <- stream.run(BufrCodeFlagAggregator.aggregateToMapTyped())
        
        // 3. Reader
        reader = BufrCodeFlagReader(aggregatedMap)
        
        // 4. Assertions - Must be updated to match the new sample data
        allKeys <- ZIO.succeed(reader.getAllKeys)
        entry001003_0 <- ZIO.succeed(reader.getFirst(BufrCodeFlagKey("001003", 0)))
        entry001003_1 <- ZIO.succeed(reader.getFirst(BufrCodeFlagKey("001003", 1)))
        entry001007_1 <- ZIO.succeed(reader.getFirst(BufrCodeFlagKey("001007", 1)))
        
        // 5. Cleanup
        _ <- Files.delete(tempFile)
      } yield {
        assert(allKeys)(hasSize(equalTo(4))) && // 4 unique keys
        assert(entry001003_0)(isSome(hasField("entryName", _.entryName, equalTo("ATARCTICA")))) &&
        assert(entry001003_1)(isSome(hasField("entryName", _.entryName, equalTo("REGION I")))) &&
        assert(entry001007_1)(isSome(hasField("entryName", _.entryName, equalTo("ERS 1")))) &&
        assert(reader.getFirst(BufrCodeFlagKey("nonexistent", 99)))(isNone)
      }
    }
  )
}