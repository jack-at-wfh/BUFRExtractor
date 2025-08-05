package com.weathernexus.utilities.bufr.integration

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.*
import zio.stream.*
import com.weathernexus.utilities.bufr.parsers.table.BufrCodeFlagParser
import com.weathernexus.utilities.bufr.aggregators.*
import com.weathernexus.utilities.bufr.data.*
import com.weathernexus.utilities.common.io.*

/**
 * Integration tests for the full pipeline of reading BUFR code/flag files,
 * parsing them, and aggregating the results into a map.
 *
 * This test spec simulates the process of reading from a directory containing
 * multiple files, including handling duplicate entries across files.
 */
object BufrCodeFlagIntegrationSpec extends ZIOSpecDefault {

  val file1Content =
    """FXY,ElementName_en,CodeFigure,EntryName_en,EntryName_sub1_en,EntryName_sub2_en,Note_en,Status
      |001003,WMO REGION NUMBER/GEOGRAPHICAL AREA,0,ANTARCTICA,,,,,
      |001007,SATELLITE IDENTIFIER,1,ERS 1,,,,,
      |001007,SATELLITE IDENTIFIER,2,ERS 2,,,,,
      |""".stripMargin

  val file2Content =
    """FXY,ElementName_en,CodeFigure,EntryName_en,EntryName_sub1_en,EntryName_sub2_en,Note_en,Status
      |001003,WMO REGION NUMBER/GEOGRAPHICAL AREA,1,REGION I,,,,,
      |001007,SATELLITE IDENTIFIER,1,ERS 1 (DUPLICATE),,,,, // This entry is a duplicate of ERS 1 but with different data
      |001010,CLOUD TYPE,3,Cumulus,,,,, // This is a new, unique entry
      |""".stripMargin

  def managedTempDir: ZIO[Scope, Throwable, Path] =
    for {
      tempDir <- ZIO.succeed(Path("temp-bufr-codeFlags"))
      _ <- Files.createDirectory(tempDir)
      _ <- ZIO.addFinalizer(Files.deleteRecursive(tempDir).orDie)
    } yield tempDir

  override def spec: Spec[TestEnvironment, Any] =
    suite("BufrCodeFlagIntegrationSpec")(
      test("should correctly aggregate data from multiple files, handling duplicates") {
        ZIO.scoped {
          for {
            tempDir <- managedTempDir
            filePath1 = tempDir / "codeFlags_file1.csv"
            filePath2 = tempDir / "codeFlags_file2.csv"
            _ <- Files.writeBytes(filePath1, Chunk.fromArray(file1Content.getBytes()))
            _ <- Files.writeBytes(filePath2, Chunk.fromArray(file2Content.getBytes()))

            stream = ZStream.fromIterable(List(filePath1.toString, filePath2.toString))
              .flatMap(pathString =>
                ZStream.fromFileName(pathString)
                  .via(ZPipeline.utf8Decode)
                  .via(ZPipeline.splitLines)
                  .drop(1)
                  .zipWithIndex
                  .map { case (line, index) =>
                    FileRow(pathString, (index + 2).toInt, line)
                  }
              )
              .via(BufrCodeFlagParser())

            aggregatedMap <- stream.run(BufrCodeFlagAggregator.aggregateToMap())

          } yield {
            assert(aggregatedMap.keys)(hasSize(equalTo(5))) &&
            assert(aggregatedMap.get(BufrCodeFlagKey("001007", 1)))(isSome(hasField("entryName", _.head.entryName, equalTo("ERS 1 (DUPLICATE)")))) &&
            assert(aggregatedMap.get(BufrCodeFlagKey("001003", 0)))(isSome(hasField("entryName", _.head.entryName, equalTo("ANTARCTICA")))) &&
            assert(aggregatedMap.get(BufrCodeFlagKey("001003", 1)))(isSome(hasField("entryName", _.head.entryName, equalTo("REGION I")))) &&
            assert(aggregatedMap.get(BufrCodeFlagKey("001010", 3)))(isSome(hasField("entryName", _.head.entryName, equalTo("Cumulus"))))
          }
        }
      }
    )
}
