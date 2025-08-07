package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import zio.nio.charset.Charset

trait FileToRowsPipeline {
  def pipeline: ZPipeline[Any, Throwable, Path, FileRow]
}

object FileToRowsPipeline {

  // This is the correct live layer for the FileToRowsPipeline service
  def live: ZLayer[FileService, Nothing, FileToRowsPipeline] =
    ZLayer.fromFunction(FileToRowsPipelineImpl(_))

  // This is the correct accessor to get the pipeline from the environment
  def pipeline: ZIO[FileToRowsPipeline, Nothing, ZPipeline[Any, Throwable, Path, FileRow]] =
    ZIO.serviceWith[FileToRowsPipeline](_.pipeline)
}