package com.weathernexus.utilities.common.io

import zio.*
import zio.stream.*
import zio.nio.file.*
import java.io.IOException

object FileNameStream {

  /**
   * Creates a ZStream that processes a list of filenames, verifying the existence of each one.
   *
   * This stream emits the full, verified path of each file that exists. If a file from the
   * input list does not exist at the specified path, the stream will fail with a `java.io.FileNotFoundException`
   * at that point. This makes the stream "fail-fast" and ensures that subsequent consumers
   * of the stream can assume all received paths correspond to existing files.
   *
   * The stream is built from the provided `filenames` list. Each filename is prepended
   * with the `basePath` if one is provided. The existence check is performed asynchronously
   * using ZIO's `Files.exists` which leverages non-blocking I/O.
   *
   * @param filenames A `List[String]` of relative or absolute filenames to be processed.
   * These are the filenames that will be checked for existence.
   * @param basePath An optional `String` representing a base directory path. If provided,
   * it will be prepended to each filename in the `filenames` list.
   * Defaults to an empty string, in which case filenames are treated as-is.
   * @return A `ZStream[Any, Throwable, Path]` that emits the full path
   * of files that were successfully verified to exist.
   * - `Any`: This stream has no dependencies on a ZIO environment.
   * - `Throwable`: The stream's error channel can fail with a `java.io.FileNotFoundException`
   * if a file does not exist, or an `IOException` if a filesystem
   * I/O error occurs during the existence check.
   * - `Path`: The stream emits the full path of each existing file.
   */

  def apply(filenames: List[String], basePath: String = ""): ZStream[Any, Throwable, Path] =
    ZStream.fromIterable(filenames)
      .mapZIO { filename =>
        val fullPath = if (basePath.nonEmpty) s"$basePath/$filename" else filename
        val path = Path(fullPath)
        Files.exists(path).flatMap { exists =>
          if (exists) ZIO.succeed(path) // Now returns Path instead of String
          else ZIO.fail(new java.io.FileNotFoundException(s"File not found: $fullPath"))
        }
      }
}