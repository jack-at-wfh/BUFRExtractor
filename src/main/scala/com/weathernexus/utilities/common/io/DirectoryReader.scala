package com.weathernexus.utilities.common.io

import zio.*
import zio.nio.file.*
import zio.stream.*
import java.io.IOException

/**
 * `DirectoryReader` provides functionality to read the contents of a specified directory.
 * It's designed to work with ZIO for asynchronous and error-handling capabilities.
 *
 * @param directoryPath The string path to the directory to be read.
 */
class DirectoryReader(directoryPath: String) {

  /**
   * Retrieves a sorted list of file names (excluding directories) from the specified directory.
   *
   * This method performs the following checks and operations:
   * <ul>
   * <li>Verifies that the provided `directoryPath` exists.</li>
   * <li>Verifies that the provided `directoryPath` is indeed a directory.</li>
   * <li>Lists all entries within the directory.</li>
   * <li>Filters out only regular files (excluding subdirectories).</li>
   * <li>Extracts the file names as strings.</li>
   * <li>Sorts the file names alphabetically.</li>
   * </ul>
   *
   * @return A `ZIO[Any, Throwable, List[String]]` (aliased as `Task[List[String]]`)
   * that will succeed with a sorted `List` of file names (as `String`s)
   * or fail with a `Throwable` if:
   * <ul>
   * <li>The `directoryPath` does not exist (`IllegalArgumentException`).</li>
   * <li>The `directoryPath` is not a directory (`IllegalArgumentException`).</li>
   * <li>Any underlying I/O error occurs during the file system operations.</li>
   * </ul>
   */
  
  def getFileNames: Task[List[String]] =
    for {
      path <- ZIO.attempt(Path(directoryPath))
      exists <- Files.exists(path)
      _ <- ZIO.fail(new IllegalArgumentException(s"Directory does not exist: $directoryPath"))
           .unless(exists)
      isDir <- Files.isDirectory(path)
      _ <- ZIO.fail(new IllegalArgumentException(s"Path is not a directory: $directoryPath"))
           .unless(isDir)
      paths <- Files.list(path).runCollect
      fileNames <- ZIO.foreach(paths.toList) { path =>
        Files.isRegularFile(path).map(isFile => if (isFile) Some(path.filename.toString) else None)
      }
      sortedNames = fileNames.flatten.sorted
    } yield sortedNames
}
