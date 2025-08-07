package com.weathernexus.utilities.common.io

import zio.*
import zio.nio.file.*
import java.io.IOException

object TestFileService {
  /**
   * Creates a mock FileService layer for testing.
   *
   * @param existingPaths A set of paths that the mock service will report as existing.
   */
  def live(existingPaths: Set[Path]): ZLayer[Any, Nothing, FileService] =
    ZLayer.succeed(new FileService {
      override def exists(path: Path): ZIO[Any, IOException, Boolean] =
        ZIO.succeed(existingPaths.contains(path))
    })
}