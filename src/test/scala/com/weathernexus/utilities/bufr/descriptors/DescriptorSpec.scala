package com.weathernexus.utilities.bufr.descriptors

import zio._
import zio.json._
import zio.stream._
import zio.test._
import zio.test.Assertion._

object DescriptorSpec extends ZIOSpecDefault {

  val shouldIdentifyDescriptorTypesCorrectly =
    test("should identify descriptor types correctly") {
      val testCodes = List(
        ("001001", true, false, false, false),   // Element descriptor (F=0)
        ("101001", false, true, false, false),   // Replication descriptor (F=1)
        ("201001", false, false, true, false),   // Operator descriptor (F=2)
        ("301001", false, false, false, true)    // Sequence descriptor (F=3)
      )

      ZIO.foreach(testCodes) { case (fxyCode, isElement, isReplication, isOperator, isSequence) =>
        ZIO.succeed {
          val desc = DescriptorCode.fromFXY(fxyCode).get
          assertTrue(
            desc.isElementDescriptor == isElement,
            desc.isReplicationDescriptor == isReplication,
            desc.isOperatorDescriptor == isOperator,
            desc.isSequenceDescriptor == isSequence
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldHandleInvalidFXYCodes =
    test("should handle invalid FXY codes") {
      val invalidCodes = List("", "123", "12345", "1234567", "abcdef", "12345x", "123-456")

      ZIO.succeed {
        assertTrue(
          invalidCodes.forall(code => DescriptorCode.fromFXY(code).isEmpty)
        )
      }
    }

  val applyMethodShouldWorkLikeFromFXY =
    test("apply method should work identically to fromFXY") {
      // Test with a valid code
      val validCode = "001001"
      val resultFromApply = DescriptorCode(validCode)
      val resultFromFromFXY = DescriptorCode.fromFXY(validCode)

      // Test with an invalid code
      val invalidCode = "invalid"
      val invalidResultFromApply = DescriptorCode(invalidCode)
      val invalidResultFromFromFXY = DescriptorCode.fromFXY(invalidCode)

      assertTrue(
        resultFromApply == resultFromFromFXY,
        resultFromApply.isDefined,
        resultFromApply.get.f == 0,
        invalidResultFromApply == invalidResultFromFromFXY,
        invalidResultFromApply.isEmpty
      )
    }

  val shouldProvideCorrectStringRepresentations =
    test("should provide correct string representations") {
      val desc = DescriptorCode(f = 0, x = 1, y = 1)
      assertTrue(
        desc.toFXY == "001001",
        desc.toString == "001001"
      )
    }

  val descriptorCodeFunctionalityTestSuite =
    suite("DescriptorCode functionality")(
      shouldIdentifyDescriptorTypesCorrectly,
      shouldHandleInvalidFXYCodes,
      applyMethodShouldWorkLikeFromFXY,
      shouldProvideCorrectStringRepresentations
    )

  def spec = suite("DescriptorSpec")(
    descriptorCodeFunctionalityTestSuite
  )
}