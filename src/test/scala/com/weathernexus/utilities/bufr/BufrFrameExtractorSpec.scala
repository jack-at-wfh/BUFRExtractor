package com.weathernexus.utilities.bufr

import zio._
import zio.test._
import zio.test.Assertion._
import zio.stream._

import java.io.IOException

object BufrFrameExtractorSpec extends ZIOSpecDefault {

  // Helper methods for generating test data remain the same
  def bufrHeader: Chunk[Byte] = Chunk('B', 'U', 'F', 'R').map(_.toByte)
  def sentinel: Chunk[Byte] = Chunk('7', '7', '7', '7').map(_.toByte)

  def randomPayload(size: Int): UIO[Chunk[Byte]] =
    Random.nextBytes(size)

  def bufrTestData(payload: Chunk[Byte]): Chunk[Byte] = {
    val noise1 = Chunk[Byte](1, 2, 3, 4, 5)
    val noise2 = Chunk[Byte](9, 8, 7)
    noise1 ++ bufrHeader ++ payload ++ sentinel ++ noise2
  }

  def expectedBufr(payload: Chunk[Byte]): Chunk[Byte] =
    bufrHeader ++ payload ++ sentinel

  // --- New Helper Method for providing the service layer ---
  def extractAndCollect(input: ZStream[Any, Throwable, Byte]): ZIO[BufrFrameExtractor, Throwable, Chunk[Chunk[Byte]]] =
    ZIO.serviceWithZIO[BufrFrameExtractor](_.extractBUFR(input).runCollect)

  // New Test Case 1: Ensures a complete BUFR message at the very end of the stream is extracted.
  val extractsCompleteBufrAtEndOfStream =
    test("extractBUFR extracts a complete message that ends the stream") {
      for {
        payload <- randomPayload(10)
        input = ZStream.fromChunk(bufrHeader ++ payload ++ sentinel)
        result <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(1)) &&
        exists(equalTo(expectedBufr(payload)))
      )
    }

  // New Test Case 2: Ensures an incomplete BUFR message at the end of the stream is ignored.
  val ignoresIncompleteBufrAtEndOfStream =
    test("extractBUFR ignores an incomplete message when the stream ends") {
      for {
        payload <- randomPayload(10)
        // Stream ends after the header but before the sentinel
        input = ZStream.fromChunk(bufrHeader ++ payload)
        result <- extractAndCollect(input)
      } yield assert(result)(isEmpty)
    }

  val extractBUFRwithRandom5kPayload =
    test("extractBUFR should emit a BUFR message with a 5K random payload") {
      for {
        payload <- randomPayload(5120)
        input   = ZStream.fromChunk(bufrTestData(payload))
        result  <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(1)) &&
        exists(equalTo(expectedBufr(payload)))
      )
    }

  val extractBUFRfindsTwoBUFRmessagesBackToBack =
    test("extractBUFR finds two BUFR messages back-to-back in a stream") {
      for {
        payload1 <- randomPayload(16)
        payload2 <- randomPayload(24)
        streamData = bufrHeader ++ payload1 ++ sentinel ++ bufrHeader ++ payload2 ++ sentinel
        input = ZStream.fromChunk(streamData)
        result <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(2)) &&
        contains(expectedBufr(payload1)) &&
        contains(expectedBufr(payload2))
      )
    }

  val extractBUFRfindsBUFRmessageWithSentinelSplitAcrossChunks =
    test("extractBUFR finds BUFR message with sentinel split across chunks") {
      for {
        payload <- randomPayload(10)
        chunk1 = bufrHeader ++ payload ++ Chunk('7', '7').map(_.toByte)
        chunk2 = Chunk('7', '7').map(_.toByte)
        input = ZStream.fromChunks(chunk1, chunk2)
        result <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(1)) &&
        exists(equalTo(bufrHeader ++ payload ++ sentinel))
      )
    }

  val ignoresNoiseBeforeAndAfterBUFRmessage =
    test("Ignores noise before and after a BUFR message") {
      val noise1 = Chunk[Byte](99, 100, 101)
      val noise2 = Chunk[Byte](77, 88)
      val payload = Chunk[Byte](1, 2, 3)
      val streamData = noise1 ++ bufrHeader ++ payload ++ sentinel ++ noise2
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(equalTo(Chunk(expectedBufr(payload))))
    }

  val ignoresNoiseBetweenTwoBUFRmessages =
    test("Ignores noise between two BUFR messages") {
      val noise = Chunk[Byte](55, 66, 77)
      val payload1 = Chunk[Byte](4, 5, 6)
      val payload2 = Chunk[Byte](7, 8)
      val streamData = bufrHeader ++ payload1 ++ sentinel ++ noise ++ bufrHeader ++ payload2 ++ sentinel
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(2)) &&
        contains(expectedBufr(payload1)) &&
        contains(expectedBufr(payload2))
      )
    }

  val ignoresPartialBUFRmessages =
    test("Ignores partial/incomplete BUFR message (no sentinel)") {
      val payload = Chunk[Byte](1, 2, 3)
      val streamData = bufrHeader ++ payload // no sentinel
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(isEmpty)
    }

  val ignoresCorruptedHeaderOrSentinel =
    test("Ignores corrupted header or sentinel") {
      val corruptHeader = Chunk('B', 'U', 'F', 'X').map(_.toByte)
      val corruptSentinel = Chunk('7', '7', '7', '8').map(_.toByte)
      val payload = Chunk[Byte](1, 2, 3)
      val streamData =
        corruptHeader ++ payload ++ corruptSentinel ++
        bufrHeader ++ payload ++ sentinel // Only this one is valid
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(equalTo(Chunk(expectedBufr(payload))))
    }

  val handlesEmbeddedHeaderInPayload =
    test("Handles embedded header in payload (should not split)") {
      val payload = Chunk[Byte](1, 2) ++ bufrHeader ++ Chunk[Byte](3, 4)
      val streamData = bufrHeader ++ payload ++ sentinel
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(equalTo(Chunk(expectedBufr(payload))))
    }

  val handlesEmbeddedSentinelInPayload =
    test("Handles embedded sentinel in payload (should split)") {
      val payload = Chunk[Byte](1, 2) ++ sentinel ++ Chunk[Byte](3, 4)
      val streamData = bufrHeader ++ payload ++ sentinel
      val input = ZStream.fromChunk(streamData)

      // The extractor will emit only up to the first sentinel after the header
      val expected = bufrHeader ++ Chunk[Byte](1, 2) ++ sentinel
      for {
        result <- extractAndCollect(input)
      } yield assert(result)(equalTo(Chunk(expected)))
    }

  val backToBackBUFRmessagesWithNoise =
    test("Back-to-back BUFR messages with noise at start and end") {
      val noise1 = Chunk[Byte](1, 9, 2)
      val noise2 = Chunk[Byte](8, 7)
      val payload1 = Chunk[Byte](11, 12)
      val payload2 = Chunk[Byte](13, 14)
      val streamData = noise1 ++ bufrHeader ++ payload1 ++ sentinel ++ bufrHeader ++ payload2 ++ sentinel ++ noise2
      val input = ZStream.fromChunk(streamData)

      for {
        result <- extractAndCollect(input)
      } yield assert(result)(
        hasSize(equalTo(2)) &&
        contains(expectedBufr(payload1)) &&
        contains(expectedBufr(payload2))
      )
    }
    
  def spec: Spec[Any, Throwable] =
    suite("BufrFrameExtractorSpec")(
      extractsCompleteBufrAtEndOfStream,
      ignoresIncompleteBufrAtEndOfStream,
      extractBUFRwithRandom5kPayload,
      extractBUFRfindsTwoBUFRmessagesBackToBack,
      extractBUFRfindsBUFRmessageWithSentinelSplitAcrossChunks,
      ignoresNoiseBeforeAndAfterBUFRmessage,
      ignoresNoiseBetweenTwoBUFRmessages,
      ignoresPartialBUFRmessages,
      ignoresCorruptedHeaderOrSentinel,
      handlesEmbeddedHeaderInPayload,
      handlesEmbeddedSentinelInPayload,
      backToBackBUFRmessagesWithNoise,
    ).provideShared(BufrFrameExtractor.live)
}