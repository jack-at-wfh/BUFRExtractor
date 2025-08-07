package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException

// Create your own file service trait
trait FileService {
  def exists(path: Path): ZIO[Any, IOException, Boolean]
}

// Implementation using ZIO NIO Files
case class FileServiceLive() extends FileService {
  override def exists(path: Path): ZIO[Any, IOException, Boolean] = 
    Files.exists(path)
}

object FileService {
  def live: ZLayer[Any, Nothing, FileService] = 
    ZLayer.succeed(FileServiceLive())
    
  def exists(path: Path): ZIO[FileService, IOException, Boolean] =
    ZIO.serviceWithZIO[FileService](_.exists(path))
}