package com.weathernexus.utilities.common.io

import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.*
import java.io.IOException

object FileServiceLiveSpec extends ZIOSpecDefault {

  def spec = suite("FileServiceLive")(
    test("should return true for an existing file") {
      for {
        // Corrected createTempFile call using named parameters
        tempFile <- Files.createTempFile(suffix = ".txt", prefix = Some("temp-file"), fileAttributes = Iterable.empty)
        result <- FileService.exists(tempFile).provide(FileService.live)
        _ <- Files.deleteIfExists(tempFile)
      } yield assert(result)(isTrue)
    },

    test("should return false for a non-existent file") {
      val nonExistentPath = Path("nonexistent-file.txt")
      val effect = FileService.exists(nonExistentPath).provide(FileService.live)
      assertZIO(effect)(isFalse)
    }
  )
}