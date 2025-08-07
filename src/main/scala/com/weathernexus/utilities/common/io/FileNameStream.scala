package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException

trait FileNameStream {
  def streamFiles: ZStream[Any, Throwable, Path]
}

object FileNameStream {  
  def streamFiles: ZStream[FileNameStream, Throwable, Path] =
    ZStream.serviceWithStream[FileNameStream](_.streamFiles)
}