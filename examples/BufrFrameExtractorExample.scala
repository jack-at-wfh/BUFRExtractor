import zio._
import zio.stream._
import com.bufrtools.BufrFrameExtractor

object BufrFrameExtractorExample extends ZIOAppDefault {

// Simulate an input stream of bytes containing multiple BUFR messages
// For demonstration, we create a simple stream with two messages
// and some interleaved non-BUFR data.
    val bufrMessage1 = Chunk('B', 'U', 'F', 'R', '1', '2', '3', '4', '5', '6', '7', '7', '7', '7').map(.toByte)
    val bufrMessage2 = Chunk('B', 'U', 'F', 'R', 'A', 'B', 'C', 'D', 'E', 'F', '7', '7', '7', '7').map(.toByte)
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