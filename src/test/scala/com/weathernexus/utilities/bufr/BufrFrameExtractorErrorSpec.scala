package com.weathernexus.utilities.bufr

import zio._
import zio.test._
import zio.test.Assertion._
import zio.stream._

object BufrFrameExtractorErrorSpec extends ZIOSpecDefault {
  val extractorFailsStreamWhenInputStreamFails =
    test("Extractor fails the stream when the input stream fails") {
      val stream = ZStream.fromChunk(Chunk[Byte](1,2,3)) ++ ZStream.fail(new RuntimeException("boom!"))
      val result = BufrFrameExtractor.extractBUFR(stream).runCollect.exit
      result.map(exit => assert(exit)(
        fails(isSubtype[RuntimeException](anything))
      ))
    }
  override def spec = suite("BufrFrameExtractor error handling")(
    extractorFailsStreamWhenInputStreamFails
  )
}