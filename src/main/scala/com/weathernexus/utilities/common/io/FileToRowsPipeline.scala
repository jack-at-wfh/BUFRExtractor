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
   * Creates a `ZPipeline` that transforms a stream of filenames into a stream of `FileRow` objects.
   *
   * This pipeline is designed for high-throughput file processing. It concurrently reads up to
   * four files at a time and emits their lines as `FileRow` objects into the output stream.
   * Due to the parallel, unordered nature of the pipeline, lines from different files may be
   * interleaved in the output stream.
   *
   * For each filename received from the input stream, the pipeline performs the following actions:
   * 1. Opens the file using non-blocking I/O.
   * 2. Reads the file line by line, assuming UTF-8 encoding.
   * 3. For each line, it creates a `FileRow` object, where the `filename` field is the base name
   * of the file (e.g., "file.txt" from "/path/to/file.txt") and the `lineNumber` is 1-based.
   * 4. All `FileRow` objects for a given file are collected and then flattened into the output stream.
   *
   * The pipeline will fail as soon as any single file cannot be processed (e.g., if the file
   * does not exist or an I/O error occurs), halting the entire stream.
   *
   * @return A `ZPipeline[Any, Throwable, String, FileRow]`
   * - `Any`: The pipeline has no ZIO environment dependencies.
   * - `Throwable`: The error channel can contain exceptions like `java.io.FileNotFoundException`
   * or other `java.io.IOException` instances if a file read operation fails.
   * - `String`: The expected input type of the stream, representing file paths.
   * - `FileRow`: The output type of the stream, where each element represents a line of a file.
   */
  def apply(): ZPipeline[Any, Throwable, String, FileRow] =
    ZPipeline.mapZIOParUnordered(4) { (filename: String) =>
      ZIO.attempt {
        val path = Path(filename)
        // Extract just the filename without path for the FileRow
        val shortFilename = filename.split("[/\\\\]").lastOption.getOrElse(filename)
        
        Files.lines(path, Charset.Standard.utf8)
          .zipWithIndex
          .map { case (line, index) =>
            FileRow(shortFilename, (index + 1).toInt, line)
          }
          .runCollect
          .map(_.toList)
      }.flatten
    }.flattenIterables
}