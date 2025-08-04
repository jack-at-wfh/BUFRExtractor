package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import zio.nio.charset.Charset
import zio.test.*
import zio.test.Assertion.*

import java.io.IOException

object FileToRowsPipelineSpec extends ZIOSpecDefault {

  def spec = suite("FileToRowsPipeline")(
    test("should read lines from a single file") {
      for {
        tempFile <- createTempFileWithContent("test.txt", List("line1", "line2", "line3"))
        pipeline = FileToRowsPipeline()
        result <- ZStream.succeed(tempFile) // Changed to ZStream.succeed(Path)
          .via(pipeline)
          .runCollect
        // Simplify filename extraction using Path.filename
        actualFilename = tempFile.filename.toString 
        _ <- Files.deleteIfExists(tempFile)
      } yield {
        val rows = result.toList
        assert(rows)(hasSize(equalTo(3))) &&
        assert(rows.map(_.content))(equalTo(List("line1", "line2", "line3"))) &&
        assert(rows.map(_.lineNumber))(equalTo(List(1, 2, 3))) &&
        assert(rows.map(_.filename))(forall(equalTo(actualFilename)))
      }
    },

    test("should handle empty file") {
      for {
        tempFile <- createTempFileWithContent("empty.txt", List.empty)
        pipeline = FileToRowsPipeline()
        result <- ZStream.succeed(tempFile) // Changed to ZStream.succeed(Path)
          .via(pipeline)
          .runCollect
        _ <- Files.deleteIfExists(tempFile)
      } yield assert(result.toList)(isEmpty)
    },

    test("should handle multiple files") {
      for {
        tempFile1 <- createTempFileWithContent("file1.txt", List("a", "b"))
        tempFile2 <- createTempFileWithContent("file2.txt", List("x", "y", "z"))
        // Simplify filename extraction using Path.filename
        actualFilename1 = tempFile1.filename.toString
        actualFilename2 = tempFile2.filename.toString
        pipeline = FileToRowsPipeline()
        result <- ZStream.fromIterable(List(tempFile1, tempFile2)) // Changed to a List of Paths
          .via(pipeline)
          .runCollect
        _ <- Files.deleteIfExists(tempFile1)
        _ <- Files.deleteIfExists(tempFile2)
      } yield {
        val rows = result.toList
        assert(rows)(hasSize(equalTo(5))) &&
        assert(rows.count(_.filename == actualFilename1))(equalTo(2)) &&
        assert(rows.count(_.filename == actualFilename2))(equalTo(3))
      }
    },

    test("should handle file with blank lines") {
      for {
        tempFile <- createTempFileWithContent("blanks.txt", List("line1", "", "line3", ""))
        pipeline = FileToRowsPipeline()
        result <- ZStream.succeed(tempFile) // Changed to ZStream.succeed(Path)
          .via(pipeline)
          .runCollect
        _ <- Files.deleteIfExists(tempFile)
      } yield {
        val rows = result.toList
        assert(rows)(hasSize(equalTo(4))) &&
        assert(rows.map(_.content))(equalTo(List("line1", "", "line3", ""))) &&
        assert(rows.map(_.lineNumber))(equalTo(List(1, 2, 3, 4)))
      }
    },

    test("should fail for non-existent file") {
      val pipeline = FileToRowsPipeline()
      assertZIO(
        ZStream.succeed(Path("nonexistent.txt")) // Changed to ZStream.succeed(Path)
          .via(pipeline)
          .runCollect
          .exit
      )(fails(anything))
    },

    test("should extract filename correctly from full path") {
      for {
        tempDir <- createTempDirectory
        filePath = tempDir / "nested-file.txt"
        _ <- Files.createFile(filePath)
        _ <- Files.writeLines(filePath, List("content"))
        pipeline = FileToRowsPipeline()
        result <- ZStream.succeed(filePath) // Changed to ZStream.succeed(Path)
          .via(pipeline)
          .runCollect
        _ <- cleanupTempDirectory(tempDir)
      } yield {
        val rows = result.toList
        assert(rows)(hasSize(equalTo(1))) &&
        assert(rows.head.filename)(equalTo("nested-file.txt"))
      }
    }
  )

  // Helper methods remain the same. They already return Path objects.
  private def createTempDirectory: Task[Path] =
    Files.createTempDirectory(Some("test-dir"), Seq.empty)

  private def createTempFileWithContent(filename: String, lines: List[String]): Task[Path] = {
    val parts = filename.split("\\.")
    val prefix = if (parts.length > 1) parts.dropRight(1).mkString(".") else filename
    val suffix = if (parts.length > 1) Some(s".${parts.last}") else None
    
    for {
      tempFile <- Files.createTempFile(prefix, suffix, Seq.empty)
      _ <- Files.writeLines(tempFile, lines)
    } yield tempFile
  }

  private def cleanupTempDirectory(dir: Path): Task[Unit] =
    for {
      contents <- Files.list(dir).runCollect
      _ <- ZIO.foreach(contents)(Files.deleteIfExists(_))
      _ <- Files.deleteIfExists(dir)
    } yield ()
}