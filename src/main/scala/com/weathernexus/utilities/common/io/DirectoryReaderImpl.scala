package com.weathernexus.utilities.common.io

import zio.*
import zio.nio.file.*
import zio.stream.*
import java.io.IOException

final private case class DirectoryReaderImpl(directoryPath: String) extends DirectoryReader {
  
  override def getFileNames: Task[List[String]] =
    for {
      path <- ZIO.attempt(Path(directoryPath))
      exists <- Files.exists(path)
      _ <- ZIO.fail(new IllegalArgumentException(s"Directory does not exist: $directoryPath")).unless(exists)
      isDir <- Files.isDirectory(path)
      _ <- ZIO.fail(new IllegalArgumentException(s"Path is not a directory: $directoryPath")).unless(isDir)
      paths <- Files.list(path).runCollect
      fileNames <- ZIO.foreach(paths.toList) { path =>
        Files.isRegularFile(path).map(isFile => if (isFile) Some(path.filename.toString) else None)
      }
      sortedNames = fileNames.flatten.sorted
    } yield sortedNames
}

object DirectoryReaderImpl {
  def live(directoryPath: String): ZLayer[Any, Nothing, DirectoryReader] =
    ZLayer.succeed(DirectoryReaderImpl(directoryPath))
}
