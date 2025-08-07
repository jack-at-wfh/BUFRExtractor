package com.weathernexus.utilities.common.io

import zio.*
import zio.nio.file.*
import zio.stream.*
import java.io.IOException

trait DirectoryReader {
  def getFileNames: Task[List[String]]
}

object DirectoryReader {
  def getFileNames: ZIO[DirectoryReader, Throwable, List[String]] =
    ZIO.serviceWithZIO[DirectoryReader](_.getFileNames)
}