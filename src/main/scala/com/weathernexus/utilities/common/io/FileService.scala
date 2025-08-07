package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.charset.Charset
import zio.nio.file.*
import java.io.IOException

// Create your own file service trait
trait FileService {
  def exists(path: Path): ZIO[Any, IOException, Boolean]
  def readAllLines(path: Path, charset: Charset): ZIO[Any, IOException, List[String]] 
}

// Implementation using ZIO NIO Files
case class FileServiceLive() extends FileService {
  override def readAllLines(path: Path, charset: Charset): ZIO[Any, IOException, List[String]] =
    Files.readAllLines(path, charset).map(_.toList)

  override def exists(path: Path): ZIO[Any, IOException, Boolean] = 
    Files.exists(path)
}

object FileService {
  def live: ZLayer[Any, Nothing, FileService] = 
    ZLayer.succeed(FileServiceLive())
    
  def exists(path: Path): ZIO[FileService, IOException, Boolean] =
    ZIO.serviceWithZIO[FileService](_.exists(path))
}