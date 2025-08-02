package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException
import zio.test.*
import zio.test.Assertion.*

object FileNameStreamSpec extends ZIOSpecDefault {

  def spec = suite("FileNameStream")(
    test("should emit existing files without base path") {
      for {
        tempFile1 <- createTempFile("test1", ".txt")
        tempFile2 <- createTempFile("test2", ".txt")
        filenames = List(tempFile1.toString, tempFile2.toString)

        result <- FileNameStream(filenames).runCollect
        _ <- cleanupFiles(List(tempFile1, tempFile2))
      } yield assert(result)(equalTo(Chunk(tempFile1, tempFile2))) // Assert against Paths
    },

    test("should emit existing files with base path") {
      for {
        tempDir <- createTempDirectory
        file1 <- Files.createFile(tempDir / "file1.txt")
        file2 <- Files.createFile(tempDir / "file2.txt")

        filenames = List("file1.txt", "file2.txt")
        result <- FileNameStream(filenames, tempDir.toString).runCollect
        expectedPaths = Chunk(tempDir / "file1.txt", tempDir / "file2.txt") // Use Path objects
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(result)(equalTo(expectedPaths)) // Assert against Paths
    },

    test("should fail for non-existent file") {
      val filenames = List("nonexistent.txt")
      assertZIO(FileNameStream(filenames).runCollect.exit)(
        fails(isSubtype[java.io.FileNotFoundException](anything))
      )
    },

    test("should emit existing files and fail on first non-existent file") {
      for {
        tempFile <- createTempFile("existing", ".txt")
        filenames = List(tempFile.toString, "nonexistent.txt")

        result <- FileNameStream(filenames).runCollect.exit
        _ <- cleanupFiles(List(tempFile))
      } yield assert(result)(fails(isSubtype[java.io.FileNotFoundException](anything)))
    },

    test("should handle empty filename list") {
      for {
        result <- FileNameStream(List.empty).runCollect
      } yield assert(result)(isEmpty) // The ZIO Stream runCollect returns a Chunk, not a List
    },

    test("should handle mixed existing and non-existing files in order") {
      for {
        tempFile1 <- createTempFile("test1", ".txt")
        tempFile2 <- createTempFile("test2", ".txt")
        // Put non-existent file in the middle
        filenames = List(tempFile1.toString, "nonexistent.txt", tempFile2.toString)

        result <- FileNameStream(filenames).runCollect.exit
        _ <- cleanupFiles(List(tempFile1, tempFile2))
      } yield assert(result)(fails(isSubtype[java.io.FileNotFoundException](anything)))
    },

    test("should work with relative paths when base path is provided") {
      for {
        tempDir <- createTempDirectory
        testFile <- Files.createFile(tempDir / "relative-test.txt")
        relativeName = "relative-test.txt"
        
        result <- FileNameStream(List(relativeName), tempDir.toString).runCollect
        expectedPath = Chunk(tempDir / "relative-test.txt") // Use Path objects
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(result)(equalTo(expectedPath))
    }
  )

  // Helper methods for test setup
  private def createTempDirectory: Task[Path] =
    Files.createTempDirectory(Some("test-dir"), Seq.empty)

  private def createTempFile(prefix: String, suffix: String): Task[Path] =
    Files.createTempFile(prefix, Some(suffix), Seq.empty)

  private def cleanupFiles(files: List[Path]): Task[Unit] =
    ZIO.foreach(files)(Files.deleteIfExists(_)).unit

  private def cleanupTempDirectory(dir: Path): Task[Unit] =
    for {
      contents <- Files.list(dir).runCollect
      _ <- ZIO.foreach(contents)(Files.deleteIfExists(_))
      _ <- Files.deleteIfExists(dir)
    } yield ()
}