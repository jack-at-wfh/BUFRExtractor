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
        ("001001", classOf[DescriptorCode.ElementDescriptor]),     // Element descriptor (F=0)
        ("101001", classOf[DescriptorCode.ReplicationDescriptor]), // Replication descriptor (F=1)
        ("201001", classOf[DescriptorCode.OperatorDescriptor]),    // Operator descriptor (F=2)
        ("301001", classOf[DescriptorCode.SequenceDescriptor])     // Sequence descriptor (F=3)
      )

      ZIO.foreach(testCodes) { case (fxyCode, expectedClass) =>
        ZIO.succeed {
          val desc = DescriptorCode.fromFXY(fxyCode).get
          assertTrue(
            desc.getClass == expectedClass,
            desc.f == fxyCode.head.asDigit,
            desc.x == fxyCode.substring(1, 3).toInt,
            desc.y == fxyCode.substring(3, 6).toInt
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldHandleInvalidFXYCodes =
    test("should handle invalid FXY codes") {
      val invalidCodes = List(
        "", "123", "12345", "1234567", "abcdef", "12345x", "123-456",
        "401001" // F=4 is invalid (only 0-3 allowed)
      )

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
        resultFromApply.get.isInstanceOf[DescriptorCode.ElementDescriptor],
        invalidResultFromApply == invalidResultFromFromFXY,
        invalidResultFromApply.isEmpty
      )
    }

  val shouldProvideCorrectStringRepresentations =
    test("should provide correct string representations") {
      val desc = DescriptorCode.ElementDescriptor(x = 1, y = 1)
      assertTrue(
        desc.toFXY == "001001",
        desc.toString == "001001"
      )
    }

  val shouldCreateCorrectSubtypes =
    test("should create correct subtypes based on F value") {
      val testCases = List(
        ("001001", classOf[DescriptorCode.ElementDescriptor]),
        ("101001", classOf[DescriptorCode.ReplicationDescriptor]),
        ("201001", classOf[DescriptorCode.OperatorDescriptor]),
        ("301001", classOf[DescriptorCode.SequenceDescriptor])
      )

      ZIO.foreach(testCases) { case (fxyCode, expectedClass) =>
        ZIO.succeed {
          val desc = DescriptorCode.fromFXY(fxyCode).get
          assertTrue(desc.getClass == expectedClass)
        }
      }.map(_.reduce(_ && _))
    }

  val shouldSupportPatternMatching =
    test("should support pattern matching") {
      val descriptors = List(
        DescriptorCode.fromFXY("001001").get,
        DescriptorCode.fromFXY("101001").get,
        DescriptorCode.fromFXY("201001").get,
        DescriptorCode.fromFXY("301001").get
      )

      val results = descriptors.map {
        case _: DescriptorCode.ElementDescriptor => "element"
        case _: DescriptorCode.ReplicationDescriptor => "replication"
        case _: DescriptorCode.OperatorDescriptor => "operator"
        case _: DescriptorCode.SequenceDescriptor => "sequence"
      }

      assertTrue(results == List("element", "replication", "operator", "sequence"))
    }

  val descriptorCodeFunctionalityTestSuite =
    suite("DescriptorCode functionality")(
      shouldIdentifyDescriptorTypesCorrectly,
      shouldHandleInvalidFXYCodes,
      applyMethodShouldWorkLikeFromFXY,
      shouldProvideCorrectStringRepresentations,
      shouldCreateCorrectSubtypes,
      shouldSupportPatternMatching
    )

  def spec = suite("DescriptorSpec")(
    descriptorCodeFunctionalityTestSuite
  )
}