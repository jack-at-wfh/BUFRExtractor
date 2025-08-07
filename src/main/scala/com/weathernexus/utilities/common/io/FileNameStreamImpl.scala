package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException

final case class FileNameStreamImpl(directoryReader: DirectoryReader, fileService: FileService, basePath: String) extends FileNameStream {

  override def streamFiles: ZStream[Any, Throwable, Path] =
    ZStream.fromZIO(directoryReader.getFileNames)
      .flatMap(ZStream.fromIterable(_))
      .mapZIO { filename =>
        val fullPath = if (basePath.nonEmpty) s"$basePath/$filename" else filename
        val path = Path(fullPath)
        fileService.exists(path).flatMap { exists =>
          if (exists) ZIO.succeed(path)
          else ZIO.fail(new java.io.FileNotFoundException(s"File not found: $fullPath"))
        }
      }
}

object FileNameStreamImpl {
  def live(basePath: String): ZLayer[DirectoryReader & FileService, Throwable, FileNameStream] =
    ZLayer.fromFunction(FileNameStreamImpl(_, _, basePath))
}