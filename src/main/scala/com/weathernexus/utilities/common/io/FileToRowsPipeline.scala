package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import zio.nio.charset.Charset

/**
 * Represents a single line of a file, including its origin.
 *
 * @param filename The base name of the file (e.g., "data.csv") from which the line was read.
 * @param lineNumber The 1-based line number within the file.
 * @param content The actual text content of the line.
 */
case class FileRow(filename: String, lineNumber: Int, content: String)

object FileToRowsPipeline {

  /**
   * Creates a `ZPipeline` that transforms a stream of file paths (as Strings) into a stream of `FileRow` objects.
   * This pipeline is designed to be composed with other ZIO Stream pipelines. For each file path
   * it receives, it reads the file, converts each line into a `FileRow` object, and emits these
   * objects into the output stream. It processes up to four files concurrently in an unordered fashion.
   *
   * @return A `ZPipeline[Any, Throwable, String, FileRow]` that can be used to process file streams.
   */
// Update the apply method signature to accept a ZStream of Path
def apply(): ZPipeline[Any, Throwable, Path, FileRow] =
  ZPipeline.mapZIOParUnordered(4) { (path: Path) =>
    for {
      shortFilename <- ZIO.succeed(path.filename.toString)
      lines <- Files.readAllLines(path, Charset.Standard.utf8)
      fileRows <- ZStream.fromIterable(lines)
        .zipWithIndex
        .map { case (line, index) =>
          FileRow(shortFilename, (index + 1).toInt, line)
        }
        .runCollect
    } yield fileRows.toList
  }.flattenIterables
}