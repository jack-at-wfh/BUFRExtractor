package com.weathernexus.utilities.common.io

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.*
import java.io.IOException

object TestFileServiceSpec extends ZIOSpecDefault {

  def spec = suite("TestFileService")(
    test("should return true for paths in the mock set") {
      val mockPaths = Set(Path("/mock/file1.txt"), Path("/mock/file2.txt"))
      val effect = FileService.exists(Path("/mock/file1.txt"))
        .provide(TestFileService.live(mockPaths))

      assertZIO(effect)(isTrue)
    },

    test("should return false for paths not in the mock set") {
      val mockPaths = Set(Path("/mock/file1.txt"))
      val effect = FileService.exists(Path("/mock/file2.txt"))
        .provide(TestFileService.live(mockPaths))

      assertZIO(effect)(isFalse)
    }
  )
}