package com.weathernexus.utilities.bufr

import zio._
import zio.stream._
import zio.stream.ZChannel

/**
 * A service that extracts complete BUFR messages from a stream of raw bytes.
 * This service is designed to be provided via ZLayer for easy testability.
 */
trait BufrFrameExtractor {

  /**
   * Extracts BUFR messages from a stream of raw bytes.
   *
   * @param input The raw byte stream to process.
   * @return A stream of complete BUFR messages, each as a Chunk[Byte].
   */
  def extractBUFR(input: ZStream[Any, Throwable, Byte]): ZStream[Any, Throwable, Chunk[Byte]]
}

/**
 * Companion object for the BufrFrameExtractor service.
 * It contains the Live implementation and the ZLayer that provides it.
 */
object BufrFrameExtractor {

  // We can keep these constants here as they are part of the BUFR standard,
  // not a dependency that needs to be injected.
  val BUFR_HEADER: Chunk[Byte] = Chunk('B', 'U', 'F', 'R').map(_.toByte)
  val SENTINEL: Chunk[Byte] = Chunk('7', '7', '7', '7').map(_.toByte)
  val HEADER_LEN: Int = BUFR_HEADER.length
  val SENTINEL_LEN: Int = SENTINEL.length

  final case class Live() extends BufrFrameExtractor {

    override def extractBUFR(
      input: ZStream[Any, Throwable, Byte]
    ): ZStream[Any, Throwable, Chunk[Byte]] = {
      input
        .rechunk(1) // Work with single bytes for easier parsing
        .via(
          ZPipeline.fromChannel {
            def go(buffer: Chunk[Byte]): ZChannel[Any, Any, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Any] =
              ZChannel.readWith(
                (in: Chunk[Byte]) => {
                  val newBuffer = buffer ++ in
                  // Look for BUFR header
                  val startIdx = newBuffer.indexOfSlice(BUFR_HEADER)

                  if (startIdx < 0) {
                    // No BUFR header found, keep only the bytes that could form a header with future input.
                    val keep = newBuffer.takeRight(HEADER_LEN - 1)
                    go(keep)
                  } else {
                    // Found BUFR header, look for sentinel after the header
                    val afterHeader = newBuffer.drop(startIdx)
                    // The 'endIdx' is now relative to 'afterHeader', so we search from a position
                    // relative to the start of 'afterHeader'
                    val endIdx = afterHeader.indexOfSlice(SENTINEL, HEADER_LEN)

                    if (endIdx >= 0) {
                      // Found sentinel, extract the message
                      val bufrMsg = afterHeader.take(endIdx + SENTINEL_LEN)
                      val rest = afterHeader.drop(endIdx + SENTINEL_LEN)
                      ZChannel.write(Chunk.single(bufrMsg)) *> go(rest)
                    } else {
                      // Not enough data yet to find sentinel, keep the buffer from the start of the header.
                      go(afterHeader)
                    }
                  }
                },
                (err: Any) => err match {  // Still need test cases to cover this path
                  case throwable: Throwable => ZChannel.fail(throwable)
                  case other => ZChannel.fail(new RuntimeException(s"Unexpected error type: ${other.getClass.getSimpleName}: $other"))
                },
                (_: Any) => {
                  // This is the end of the stream, we need to check if we have a complete message
                  val startIdx = buffer.indexOfSlice(BUFR_HEADER)
                  val hasValidHeader = startIdx >= 0
                  val afterHeader = if (hasValidHeader) buffer.drop(startIdx) else Chunk.empty
                  val endIdx = if (hasValidHeader) afterHeader.indexOfSlice(SENTINEL, HEADER_LEN) else -1
                  val hasCompleteMessage = endIdx >= 0

                  (buffer.nonEmpty, hasValidHeader, hasCompleteMessage) match {
                    // Case 1: The buffer is not empty and contains a complete message.
                    case (true, true, true) =>
                      val bufrMsg = afterHeader.take(endIdx + SENTINEL_LEN)
                      ZChannel.write(Chunk.single(bufrMsg))
                    // Case 2 & 3: The buffer is empty or contains an incomplete message. In either case, we do nothing.
                    case _ =>
                      ZChannel.unit
                  }
                }
              )
            go(Chunk.empty[Byte])
          }
        )
    }
  }

  // Define the ZLayer that provides a Live implementation of our service.
  val live: ZLayer[Any, Nothing, BufrFrameExtractor] = ZLayer.succeed(Live())
}
