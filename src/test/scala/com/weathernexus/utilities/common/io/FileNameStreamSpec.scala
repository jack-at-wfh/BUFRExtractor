package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException
import zio.test.*
import zio.test.Assertion.*

object FileNameStreamSpec extends ZIOSpecDefault {

  def spec = suite("FileNameStream")(
    test("should emit existing files without a base path") {
      val mockFiles = List("file1.txt", "file2.txt")
      val basePath = "."
      // Convert the Set[Path] to a Map[Path, List[String]]
      val existingPathsMap = mockFiles.map(name => Path(s"$basePath/$name") -> List.empty[String]).toMap
      
      val effect = FileNameStream.streamFiles.runCollect
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPathsMap) // Provide the new Map here
          )
        )
      assertZIO(effect)(equalTo(Chunk(Path("./file1.txt"), Path("./file2.txt"))))
    },

    test("should emit existing files with a base path") {
      val mockFiles = List("file1.txt", "file2.txt")
      val basePath = "/data"
      // Convert the Set[Path] to a Map[Path, List[String]]
      val existingPathsMap = mockFiles.map(name => Path(s"$basePath/$name") -> List.empty[String]).toMap
      
      val effect = FileNameStream.streamFiles.runCollect
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPathsMap) // Provide the new Map here
          )
        )
      assertZIO(effect)(equalTo(Chunk(Path("/data/file1.txt"), Path("/data/file2.txt"))))
    },

    test("should correctly handle an empty base path") {
      val mockFiles = List("file1.txt")
      val basePath = ""
      val existingPaths = Map(Path("file1.txt") -> List.empty[String])

      val effect = FileNameStream.streamFiles.runCollect
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPaths)
          )
        )

      assertZIO(effect)(equalTo(Chunk(Path("file1.txt"))))
    },

    test("should fail with FileNotFoundException for a non-existent file") {
      val mockFiles = List("existing.txt", "nonexistent.txt")
      val basePath = "."
      // The mock service only contains the first file, so the second one will fail the 'exists' check.
      val existingPaths = Map(Path("./existing.txt") -> List.empty[String])

      val effect = FileNameStream.streamFiles.runCollect.exit
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPaths)
          )
        )

      // Use fails(isSubtype[...]) to assert that the effect fails with the expected exception
      assertZIO(effect)(fails(isSubtype[java.io.FileNotFoundException](anything)))
    }

  )
}