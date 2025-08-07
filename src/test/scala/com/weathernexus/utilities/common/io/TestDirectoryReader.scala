package com.weathernexus.utilities.common.io

import zio.*

/**
 * `TestDirectoryReader` is a mock implementation of the `DirectoryReader` service.
 * It's designed for testing, providing a hard-coded list of filenames without
 * performing any actual file system operations.
 */
object TestDirectoryReader {

  /**
   * The live layer provides a mock implementation that returns a specified list of files.
   *
   * @param files The list of filenames the mock reader should return.
   * @return A `ZLayer` providing a `DirectoryReader` that returns the specified list.
   */
  def live(files: List[String]): ZLayer[Any, Nothing, DirectoryReader] =
    ZLayer.succeed(new DirectoryReader {
      override def getFileNames: Task[List[String]] = ZIO.succeed(files.sorted)
    })
}