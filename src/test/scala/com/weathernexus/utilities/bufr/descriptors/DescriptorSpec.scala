package com.weathernexus.utilities.bufr.descriptors

import zio._
import zio.json._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import com.weathernexus.utilities.bufr.parsers.table.TableBParser
import com.weathernexus.utilities.bufr.descriptors.*

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
        val invalidCodes = List("", "123", "12345", "1234567", "abcdef", "12345x")
        
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

  val shouldFailWhenNoValidDescriptorCodesFound =
    test("should fail when no valid descriptor codes found") {
      val invalidFxyContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,INVALID1,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational
01,Identification,XYZ123,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Operational
02,Instrumentation,ABCDEF,Type of station,Code table,0,0,2,Code table,0,1,,,Operational"""
    
      assertZIO(TableBParser.parseTableBContent(invalidFxyContent).exit)(
        fails(hasMessage(containsString("No valid descriptor codes found in parsed entries")))
      )
    }

 
  val descriptorCodeFunctionalityTestSuite = 
    suite("DescriptorCode functionality")(
       shouldIdentifyDescriptorTypesCorrectly,
       shouldHandleInvalidFXYCodes,
       applyMethodShouldWorkLikeFromFXY,
       shouldFailWhenNoValidDescriptorCodesFound
    )

  def spec = suite("TableBParserSpec")(
    descriptorCodeFunctionalityTestSuite,
  )    
}