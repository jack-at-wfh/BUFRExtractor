package com.bufrtools

import zio._
import zio.test._
import zio.test.Assertion._
import zio.stream._

object BufrFrameExtractorPropertySpec extends ZIOSpecDefault {
  def bufrHeader: Chunk[Byte] = Chunk('B', 'U', 'F', 'R').map(_.toByte)
  def sentinel: Chunk[Byte] = Chunk('7', '7', '7', '7').map(_.toByte)

  // Generator for random payloads that do NOT contain the sentinel pattern (to avoid accidental splits)
  val genPayloadNoSentinel: Gen[Any, Chunk[Byte]] =
    Gen.chunkOfBounded(0, 50)(
      Gen.byte.filterNot(_ == '7'.toByte)
    )

  // Generator for a valid BUFR message
  val genBufrMessage: Gen[Any, Chunk[Byte]] =
    for {
      payload <- genPayloadNoSentinel
    } yield bufrHeader ++ payload ++ sentinel

  // Generator for random noise (not a BUFR header)
  val genNoise: Gen[Any, Chunk[Byte]] =
    Gen.chunkOf(
      Gen.byte.filterNot(_ == 'B'.toByte) // reduce chance of accidental header
    )

  // Generator for a stream with zero or more BUFR messages and noise
  val genBufrStream: Gen[Any, Chunk[Byte]] =
    Gen.chunkOf(
      Gen.oneOf(
        genNoise,
        genBufrMessage
      )
    ).map(_.flatten)

  // Property: All extracted messages start with the BUFR header and end with the sentinel
  val prop_valid_message_framing =
    test("All extracted messages start with BUFR header and end with sentinel") {
      check(genBufrStream) { bytes =>
        val input = ZStream.fromChunk(bytes)
        for {
          result <- BufrFrameExtractor.extractBUFR(input).runCollect
        } yield assert(result.forall(msg =>
          msg.startsWith(bufrHeader) && msg.endsWith(sentinel)
        ))(isTrue)
      }
    }

  // Property: Extractor finds all BUFR messages in a stream with only valid messages
  val prop_extracts_all_messages =
    test("Extractor finds all BUFR messages in a stream of only valid messages") {
      check(Gen.chunkOf(genBufrMessage)) { msgs =>
        val bytes = msgs.flatten
        val input = ZStream.fromChunk(bytes)
        for {
          result <- BufrFrameExtractor.extractBUFR(input).runCollect
        } yield assert(result)(equalTo(msgs))
      }
    }

  // Property: Extractor ignores noise, only extracts valid BUFR messages
  val prop_ignores_noise =
    test("Extractor ignores noise and only extracts valid BUFR messages") {
      check(
        for {
          prefix <- genNoise
          msgs   <- Gen.chunkOf(genBufrMessage)
          infix  <- genNoise
          suffix <- genNoise
        } yield prefix ++ msgs.flatten ++ infix ++ suffix
      ) { bytes =>
        val input = ZStream.fromChunk(bytes)
        for {
          result <- BufrFrameExtractor.extractBUFR(input).runCollect
        } yield assert(result.forall(msg =>
          msg.startsWith(bufrHeader) && msg.endsWith(sentinel)
        ))(isTrue)
      }
    }

  // Property: No extracted message contains the sentinel in its body (should only be at the end)
  val prop_no_embedded_sentinel =
    test("No extracted message contains the sentinel except at the end") {
      check(genBufrStream) { bytes =>
        val input = ZStream.fromChunk(bytes)
        for {
          result <- BufrFrameExtractor.extractBUFR(input).runCollect
        } yield assert(result.forall { msg =>
          // The only allowed sentinel is at the end
          val body = msg.dropRight(sentinel.length)
          !body.sliding(sentinel.length).contains(sentinel)
        })(isTrue)
      }
    }

  // Property: If there are no BUFR messages, nothing is extracted
  val prop_nothing_extracted_from_noise =
    test("If there are no BUFR messages, nothing is extracted") {
      check(genNoise) { noise =>
        val input = ZStream.fromChunk(noise)
        for {
          result <- BufrFrameExtractor.extractBUFR(input).runCollect
        } yield assert(result)(isEmpty)
      }
    }

  override def spec = suite("BufrFrameExtractor property-based tests")(
    prop_valid_message_framing,
    prop_extracts_all_messages,
    prop_ignores_noise,
    prop_no_embedded_sentinel,
    prop_nothing_extracted_from_noise
  )
}