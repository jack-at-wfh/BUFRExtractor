package com.bufrtools

import zio._
import zio.stream._

object BufrFrameExtractor {

  val BUFR_HEADER: Chunk[Byte] = Chunk('B', 'U', 'F', 'R').map(_.toByte)
  val SENTINEL: Chunk[Byte] = Chunk('7', '7', '7', '7').map(_.toByte)
  val HEADER_LEN = BUFR_HEADER.length
  val SENTINEL_LEN = SENTINEL.length

  /**
   * Extracts BUFR files from a stream of bytes.
   * Emits each complete BUFR message as a Chunk[Byte].
   */
  def extractBUFR(
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
                  // No BUFR header found, keep only last HEADER_LEN-1 bytes (for overlapping case)
                  val keep = newBuffer.takeRight(HEADER_LEN - 1)
                  go(keep)
                } else {
                  // Found BUFR header, look for sentinel after header
                  val afterHeader = newBuffer.drop(startIdx)
                  val endIdx = afterHeader.indexOfSlice(SENTINEL, HEADER_LEN)
                  if (endIdx >= 0) {
                    // Found sentinel, extract message
                    val bufrMsg = afterHeader.take(endIdx + SENTINEL_LEN)
                    val rest = afterHeader.drop(endIdx + SENTINEL_LEN)
                    ZChannel.write(Chunk.single(bufrMsg)) *> go(rest)
                  } else {
                    // Not enough data yet to find sentinel, keep buffer from startIdx
                    go(afterHeader)
                  }
                }
              },
              (err: Any) => ZChannel.fail(new RuntimeException("Stream failed")),
              (_: Any) => ZChannel.unit
            )
          go(Chunk.empty[Byte])
        }
      )
  }
}