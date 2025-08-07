package com.weathernexus.utilities.common.io

import zio.*
import zio.nio.charset.Charset
import zio.nio.file.*
import java.io.IOException

object TestFileService {

  /**
   * Creates a mock FileService layer for testing.
   *
   * @param files A map where keys are file paths and values are the lines of content to return.
   */
  def live(files: Map[Path, List[String]]): ZLayer[Any, Nothing, FileService] =
    ZLayer.succeed(new FileService {
      override def exists(path: Path): ZIO[Any, IOException, Boolean] =
        ZIO.succeed(files.contains(path))
      
      override def readAllLines(path: Path, charset: Charset): ZIO[Any, IOException, List[String]] =
        files.get(path) match {
          case Some(lines) => ZIO.succeed(lines)
          case None        => ZIO.fail(new IOException(s"File not found: $path"))
        }
    })
}