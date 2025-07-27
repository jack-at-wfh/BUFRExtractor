BUFR Frame Extractor
This project provides a utility for extracting complete BUFR (Binary Universal Form for the Representation of meteorological data) messages from a continuous stream of bytes using ZIO Streams.

🚀 Introduction
The BufrFrameExtractor object offers a robust and efficient way to parse a stream of raw bytes and identify individual BUFR messages. It's designed to handle scenarios where BUFR messages might be concatenated or embedded within a larger data stream, ensuring that only complete and valid BUFR frames (identified by their BUFR header and 7777 sentinel) are emitted.

✨ Features
Stream-based Processing: Leverages ZIO Streams for non-blocking, efficient data processing.

BUFR Message Extraction: Identifies and extracts complete BUFR messages based on standard headers and sentinels.

Robust Parsing: Handles partial reads and ensures correct message boundaries even if the stream is fragmented.

Scala & ZIO Native: Written entirely in Scala using the ZIO library, providing a functional and concurrent approach.

🛠️ Dependencies
This project relies on the ZIO library for its streaming capabilities.

To include ZIO in your build.sbt, add the following:

libraryDependencies += "dev.zio" %% "zio" % "2.0.0" // Or the latest stable ZIO version
libraryDependencies += "dev.zio" %% "zio-streams" % "2.0.0" // Or the latest stable ZIO version

📖 Usage
The core functionality is provided by the extractBUFR method, which takes a ZStream[Any, Throwable, Byte] as input and returns a ZStream[Any, Throwable, Chunk[Byte]], where each Chunk[Byte] represents a complete BUFR message.

Here's an example of how to use it:

import zio._
import zio.stream._
import com.bufrtools.BufrFrameExtractor

object Main extends ZIOAppDefault {

  // Simulate an input stream of bytes containing multiple BUFR messages
  // For demonstration, we'll create a simple stream with two messages
  // and some interleaved non-BUFR data.
  val bufrMessage1 = Chunk('B', 'U', 'F', 'R', '1', '2', '3', '4', '5', '6', '7', '7', '7', '7').map(_.toByte)
  val bufrMessage2 = Chunk('B', 'U', 'F', 'R', 'A', 'B', 'C', 'D', 'E', 'F', '7', '7', '7', '7').map(_.toByte)
  val junkData = Chunk('X', 'Y', 'Z').map(_.toByte)

  val inputStream: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(
    junkData ++ bufrMessage1 ++ junkData ++ bufrMessage2 ++ junkData
  )

  override def run: ZIO[Any, Throwable, Unit] =
    BufrFrameExtractor.extractBUFR(inputStream)
      .zipWithIndex // Add index to easily identify messages
      .foreach { case (bufrMsg, index) =>
        Console.printLine(s"Extracted BUFR Message ${index + 1}: ${new String(bufrMsg.toArray)}")
      }
      .catchAll { e =>
        Console.printLine(s"Error during extraction: ${e.getMessage}")
      }
}

Output of the example:

Extracted BUFR Message 1: BUFR1234567777
Extracted BUFR Message 2: BUFRABCDEF7777

📝 BUFR Format
This extractor specifically looks for:

Header: The sequence of bytes representing the ASCII string BUFR.

Sentinel: The sequence of bytes representing the ASCII string 7777.

A complete BUFR message is considered to be the data from the BUFR header up to and including the 7777 sentinel.

🤝 Contributing
Contributions are welcome! Please feel free to open issues or submit pull requests.

📄 License
This project is open-sourced under the MIT License.