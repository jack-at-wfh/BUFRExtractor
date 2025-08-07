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
      val existingPaths = mockFiles.map(name => Path(s"$basePath/$name")).toSet
      
      val effect = FileNameStream.streamFiles.runCollect
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPaths)
          )
        )

      assertZIO(effect)(equalTo(Chunk(Path("./file1.txt"), Path("./file2.txt"))))
    },

    test("should emit existing files with a base path") {
      val mockFiles = List("file1.txt", "file2.txt")
      val basePath = "/data"
      val existingPaths = mockFiles.map(name => Path(s"$basePath/$name")).toSet
      
      val effect = FileNameStream.streamFiles.runCollect
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPaths)
          )
        )
      
      assertZIO(effect)(equalTo(Chunk(Path("/data/file1.txt"), Path("/data/file2.txt"))))
    },

    test("should handle an empty base path correctly") {
      val mockFiles = List("file1.txt")
      val basePath = ""
      val existingPaths = mockFiles.map(Path(_)).toSet

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
      val mockFiles = List("file1.txt", "nonexistent.txt")
      val basePath = "."
      // The mock service only contains the first file, so the second one will fail the 'exists' check.
      val existingPaths = Set(Path("./file1.txt"))

      val effect = FileNameStream.streamFiles.runCollect.exit
        .provide(
          ZLayer.make[FileNameStream](
            FileNameStreamImpl.live(basePath),
            TestDirectoryReader.live(mockFiles),
            TestFileService.live(existingPaths)
          )
        )

      assertZIO(effect)(fails(isSubtype[java.io.FileNotFoundException](anything)))
    }
  )
}