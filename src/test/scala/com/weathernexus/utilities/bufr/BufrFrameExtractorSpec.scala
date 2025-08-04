package com.weathernexus.utilities.bufr

import zio._
import zio.test._
import zio.test.Assertion._
import zio.stream._

import java.io.IOException

object BufrFrameExtractorSpec extends ZIOSpecDefault {
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

  val extractBUFRwithRandom5kPayload =     
    test("extractBUFR should emit a BUFR message with a 5K random payload") {
      for {
        payload <- randomPayload(5120)
        input   =  ZStream.fromChunk(bufrTestData(payload))
        result  <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
      } yield assert(result)(
        hasSize(equalTo(1)) &&
        exists(equalTo(bufrHeader ++ payload ++ sentinel))
      )
    }

  val ignoresNoiseBeforeAndAfterBUFRmessage  =
    test("Ignores noise before and after a BUFR message") {
      val noise1 = Chunk[Byte](99, 100, 101)
      val noise2 = Chunk[Byte](77, 88)
      val payload = Chunk[Byte](1, 2, 3)
      val streamData = noise1 ++ bufrHeader ++ payload ++ sentinel ++ noise2
      val input = ZStream.fromChunk(streamData)

      for {
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
      } yield assert(result)(equalTo(Chunk(expectedBufr(payload))))
    }

  val handlesEmbeddedHeaderInPayload =
    test("Handles embedded header in payload (should not split)") {
      val payload = Chunk[Byte](1, 2) ++ bufrHeader ++ Chunk[Byte](3, 4)
      val streamData = bufrHeader ++ payload ++ sentinel
      val input = ZStream.fromChunk(streamData)

      for {
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
            result <- BufrFrameExtractor.extractBUFR(input).runCollect
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
        result <- BufrFrameExtractor.extractBUFR(input).runCollect
      } yield assert(result)(
        hasSize(equalTo(2)) &&
        contains(expectedBufr(payload1)) &&
        contains(expectedBufr(payload2))
      )
    }
  val extractBUFRshouldHandleStreamErrors = 
    test("extractBUFR should handle stream errors") {
      val badStream = ZStream.fail(new IOException("Simulated file read error"))
      val result = BufrFrameExtractor.extractBUFR(badStream).runCollect.exit
      assertZIO(result)(fails(isSubtype[IOException](anything)))
    }

  def spec: Spec[Any, Throwable] = suite("BufrFrameExtractorSpec")(
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
    extractBUFRshouldHandleStreamErrors
  )
}