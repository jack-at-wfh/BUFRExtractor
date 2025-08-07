package com.weathernexus.utilities.common.io

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.*
import java.io.IOException

object TestFileServiceSpec extends ZIOSpecDefault {

  def spec = suite("TestFileService")(
    test("should return true for paths in the mock map") {
      // Create a Map with paths and some placeholder content
      val mockFiles = Map(
        Path("/mock/file1.txt") -> List("content1"),
        Path("/mock/file2.txt") -> List("content2")
      )
      val effect = FileService.exists(Path("/mock/file1.txt"))
        .provide(TestFileService.live(mockFiles))
      assertZIO(effect)(isTrue)
    },

    test("should return false for paths not in the mock map") {
      val mockFiles = Map(Path("/mock/file1.txt") -> List("content"))
      val effect = FileService.exists(Path("/mock/file2.txt"))
        .provide(TestFileService.live(mockFiles))
      assertZIO(effect)(isFalse)
    }
  )
}