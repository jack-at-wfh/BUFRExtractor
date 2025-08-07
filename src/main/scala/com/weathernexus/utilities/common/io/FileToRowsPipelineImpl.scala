package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.charset.Charset
import zio.nio.file.Path

final case class FileToRowsPipelineImpl(files: FileService) extends FileToRowsPipeline {
  override def pipeline: ZPipeline[Any, Throwable, Path, FileRow] =
    ZPipeline.mapZIOParUnordered(4) { (path: Path) =>
      for {
        shortFilename <- ZIO.succeed(path.filename.toString)
        lines <- files.readAllLines(path, Charset.Standard.utf8)
        fileRows <- ZStream.fromIterable(lines)
          .zipWithIndex
          .map { case (line, index) =>
            FileRow(shortFilename, (index + 1).toInt, line)
          }
          .runCollect
      } yield fileRows.toList
    }.flattenIterables
}