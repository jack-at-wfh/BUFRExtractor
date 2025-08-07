package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*
import zio.nio.file.Path

// Explicitly import the object to ensure it's in scope
import com.weathernexus.utilities.common.io.FileToRowsPipeline

object FileToRowsPipelineAccessorSpec extends ZIOSpecDefault {

  // Create a mock implementation of the FileToRowsPipeline service
  final case class MockFileToRowsPipeline() extends FileToRowsPipeline {
    override def pipeline: ZPipeline[Any, Throwable, Path, FileRow] =
      // Use ZPipeline.map to create a mock pipeline that correctly
      // transforms the input type (Path) to the output type (FileRow).
      ZPipeline.map[Path, FileRow](_ => FileRow("mock.txt", 1, "mock content"))
  }

  // Create a ZLayer for the mock service
  val mockLayer: ZLayer[Any, Nothing, FileToRowsPipeline] =
    ZLayer.succeed(MockFileToRowsPipeline())

  def spec = suite("FileToRowsPipeline Accessor")(
    test("should successfully retrieve the pipeline from the environment") {
      val effect = FileToRowsPipeline.pipeline.provide(mockLayer)
      assertZIO(effect)(isSubtype[ZPipeline[Any, Throwable, Path, FileRow]](anything))
    }
  )
}