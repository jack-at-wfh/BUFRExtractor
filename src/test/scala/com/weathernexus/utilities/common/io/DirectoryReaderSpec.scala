package com.weathernexus.utilities.common.io

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.*
import java.io.IOException
import java.nio.file.NoSuchFileException

object DirectoryReaderSpec extends ZIOSpecDefault {

  def spec = suite("DirectoryReader")(
    test("should read and sort filenames from existing directory") {
      for {
        tempDir <- createTempDirectoryWithFiles("test1.txt", "zebra.txt", "alpha.txt")
        fileNames <- DirectoryReader.getFileNames.provide(DirectoryReaderImpl.live(tempDir.toString))
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(fileNames)(equalTo(List("alpha.txt", "test1.txt", "zebra.txt")))
    },

    test("should return empty list for empty directory") {
      for {
        tempDir <- createTempDirectory
        fileNames <- DirectoryReader.getFileNames.provide(DirectoryReaderImpl.live(tempDir.toString))
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(fileNames)(isEmpty)
    },

    test("should exclude subdirectories from results") {
      for {
        tempDir <- createTempDirectoryWithFilesAndDirs(
          files = List("file1.txt", "file2.txt"),
          dirs = List("subdir1", "subdir2")
        )
        fileNames <- DirectoryReader.getFileNames.provide(DirectoryReaderImpl.live(tempDir.toString))
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(fileNames)(equalTo(List("file1.txt", "file2.txt")))
    },

    test("should fail with IllegalArgumentException for non-existent directory") {
      val effect = DirectoryReader.getFileNames
        .provide(DirectoryReaderImpl.live("/path/that/does/not/exist"))
      assertZIO(effect.exit)(
        fails(isSubtype[IllegalArgumentException](anything))
      )
    },

    test("should fail with IllegalArgumentException when path is a file, not directory") {
      for {
        tempFile <- createTempFile
        effect = DirectoryReader.getFileNames
          .provide(DirectoryReaderImpl.live(tempFile.toString))
        result <- effect.exit
        _ <- Files.deleteIfExists(tempFile)
      } yield assert(result)(fails(isSubtype[IllegalArgumentException](anything)))
    },

    test("should handle directory with mixed file types") {
      for {
        tempDir <- createTempDirectoryWithFiles(
          "document.pdf", "image.jpg", "script.sh", "data.json"
        )
        fileNames <- DirectoryReader.getFileNames.provide(DirectoryReaderImpl.live(tempDir.toString))
        _ <- cleanupTempDirectory(tempDir)
      } yield assert(fileNames)(
        equalTo(List("data.json", "document.pdf", "image.jpg", "script.sh"))
      )
    },

    test("should work with actual resources directory") {
      val effect = DirectoryReader.getFileNames
        .provide(DirectoryReaderImpl.live("src/main/resources/bufr-tables/codeFlags"))
      
      assertZIO(effect)(isNonEmpty) && 
        assertZIO(effect)(forall(isNonEmptyString)) &&
        assertZIO(effect)(isSorted)
    }
  )

  // Helper methods for test setup remain the same
  private def createTempDirectory: Task[Path] =
    Files.createTempDirectory(Some("test-dir"), Seq.empty)

  private def createTempFile: Task[Path] =
    Files.createTempFile("test-file", Some(".txt"), Seq.empty)

  private def createTempDirectoryWithFiles(fileNames: String*): Task[Path] =
    for {
      tempDir <- createTempDirectory
      _ <- ZIO.foreach(fileNames) { fileName =>
        Files.createFile(tempDir / fileName)
      }
    } yield tempDir

  private def createTempDirectoryWithFilesAndDirs(
    files: List[String], 
    dirs: List[String]
  ): Task[Path] =
    for {
      tempDir <- createTempDirectory
      _ <- ZIO.foreach(files) { fileName =>
        Files.createFile(tempDir / fileName)
      }
      _ <- ZIO.foreach(dirs) { dirName =>
        Files.createDirectory(tempDir / dirName)
      }
    } yield tempDir

  private def cleanupTempDirectory(dir: Path): Task[Unit] =
    for {
      contents <- Files.list(dir).runCollect
      _ <- ZIO.foreach(contents) { path =>
        Files.deleteIfExists(path)
      }
      _ <- Files.deleteIfExists(dir)
    } yield ()

  // Custom assertions also remain the same
  private def isSorted: Assertion[List[String]] = 
    assertion("isSorted")(list => list == list.sorted)

  private def isNonEmptyString: Assertion[String] =
    assertion("isNonEmptyString")(str => str.nonEmpty)
}