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
        // Note: In BUFR/CREX, F is actually the first digit, not first two digits
        // The parsing code extracts first 2 chars as F, but logically F should be 0-3
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
        resultFromApply == resultFromFromFXY, // Assert that both methods return the same result for valid input
        resultFromApply.isDefined,             // Ensure it's not None
        resultFromApply.get.f == 0,            // Check a property to ensure it parsed correctly
        invalidResultFromApply == invalidResultFromFromFXY, // Assert same result for invalid input
        invalidResultFromApply.isEmpty         // Ensure it's None
      )
    }

  val shouldFailWhenNoValidDescriptorCodesFound =
    test("should fail when no valid descriptor codes found") {
      // CSV with valid structure but invalid FXY codes that can't be parsed into DescriptorCode
      val invalidFxyContent = """ClassNo,ClassName_en,FXY,ElementName_en,BUFR_Unit,BUFR_Scale,BUFR_ReferenceValue,BUFR_DataWidth_Bits,CREX_Unit,CREX_Scale,CREX_DataWidth_Char,Note_en,noteIDs,Status
01,Identification,INVALID1,WMO block number,Numeric,0,0,7,Numeric,0,2,,,Operational
01,Identification,XYZ123,WMO station number,Numeric,0,0,10,Numeric,0,3,,,Operational
02,Instrumentation,ABCDEF,Type of station,Code table,0,0,2,Code table,0,1,,,Operational"""
    
      assertZIO(TableBParser.parseTableBContent(invalidFxyContent).exit)(
        fails(hasMessage(containsString("No valid descriptor codes found in parsed entries")))
      )
    }
 
  val shouldEncodeDescriptorCodeToJson =
    test("should encode DescriptorCode to JSON correctly") {
      val testCases = List(
        ("001001", "element descriptor"),
        ("101001", "replication descriptor"), 
        ("201001", "operator descriptor"),
        ("301001", "sequence descriptor")
      )
      
      ZIO.foreach(testCases) { case (fxyCode, description) =>
        ZIO.succeed {
          val descriptor = DescriptorCode.fromFXY(fxyCode).get
          val json = descriptor.toJson
          
          // Verify JSON contains the case class fields (f, x, y)
          // The derived encoder will only include the case class fields, not computed methods
          assertTrue(
            json.contains("\"f\":"),
            json.contains("\"x\":"),
            json.contains("\"y\":"),
            // Verify the actual values are correct
            json.contains(s""""f":${descriptor.f}"""),
            json.contains(s""""x":${descriptor.x}"""),
            json.contains(s""""y":${descriptor.y}""")
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldDecodeJsonToDescriptorCode =
    test("should decode JSON to DescriptorCode correctly") {
      val testCases = List(
        ("""{"f":0,"x":1,"y":1}""", "001001"),
        ("""{"f":1,"x":1,"y":1}""", "101001"),
        ("""{"f":2,"x":1,"y":1}""", "201001"),
        ("""{"f":3,"x":1,"y":1}""", "301001")
      )
      
      ZIO.foreach(testCases) { case (jsonStr, expectedFxy) =>
        ZIO.succeed {
          val decodedResult = jsonStr.fromJson[DescriptorCode]
          
          assertTrue(
            decodedResult.isRight,
            decodedResult.map(_.toFXY).getOrElse("") == expectedFxy,
            decodedResult.map(_.f).getOrElse(-1) == expectedFxy.head.asDigit
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldRoundTripEncodeDecodeCorrectly =
    test("should round-trip encode/decode correctly") {
      val testCodes = List("001001", "101001", "201001", "301001", "002155", "031002")
      
      ZIO.foreach(testCodes) { fxyCode =>
        ZIO.succeed {
          val original = DescriptorCode.fromFXY(fxyCode).get
          val json = original.toJson
          val decoded = json.fromJson[DescriptorCode]
          
          assertTrue(
            decoded.isRight,
            decoded.map(_ == original).getOrElse(false),
            decoded.map(_.toFXY).getOrElse("") == fxyCode
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldHandleInvalidJsonGracefully =
    test("should handle invalid JSON gracefully") {
      val invalidJsonCases = List(
        """{"f":"invalid","x":1,"y":1}""",    // Invalid f type
        """{"f":0,"x":"invalid","y":1}""",    // Invalid x type
        """{"f":0,"x":1,"y":"invalid"}""",    // Invalid y type
        """{"f":0,"x":1}""",                  // Missing y field
        """{"invalid":"json"}""",             // Completely wrong structure
        """not json at all""",               // Not JSON
        ""                                // Empty string
      )
      
      ZIO.succeed {
        assertTrue(
          invalidJsonCases.forall { jsonStr =>
            jsonStr.fromJson[DescriptorCode].isLeft
          }
        )
      }
    }

  val shouldHandleEdgeCaseValues =
    test("should handle edge case values in JSON") {
      val edgeCases = List(
        // Test boundary values
        ("""{"f":0,"x":0,"y":0}""", "000000"),
        ("""{"f":3,"x":63,"y":255}""", "363255"),
        // Test typical BUFR values
        ("""{"f":0,"x":1,"y":7}""", "001007"),
        ("""{"f":0,"x":31,"y":31}""", "031031")
      )
      
      ZIO.foreach(edgeCases) { case (jsonStr, expectedFxy) =>
        ZIO.succeed {
          val decoded = jsonStr.fromJson[DescriptorCode]
          val reEncoded = decoded.map(_.toJson)
          val reDecoded = reEncoded.flatMap(_.fromJson[DescriptorCode])
          
          assertTrue(
            decoded.isRight,
            decoded.map(_.toFXY).getOrElse("") == expectedFxy,
            reDecoded.isRight,
            reDecoded == decoded
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldUseImplicitEncoderCorrectly =
    test("should use implicit JsonEncoder to encode DescriptorCode") {
      val testCases = List(
        ("001001", DescriptorCode(0, 1, 1)),
        ("101001", DescriptorCode(1, 1, 1)),
        ("201001", DescriptorCode(2, 1, 1)),
        ("301001", DescriptorCode(3, 1, 1))
      )
      
      ZIO.foreach(testCases) { case (expectedFxy, descriptor) =>
        ZIO.succeed {
          // This will use the implicit encoder from the companion object
          val json = descriptor.toJson
          
          // Verify the JSON structure matches what the derived encoder should produce
          assertTrue(
            json.contains("\"f\":"),
            json.contains("\"x\":"),
            json.contains("\"y\":"),
            json.contains(s""""f":${descriptor.f}"""),
            json.contains(s""""x":${descriptor.x}"""),
            json.contains(s""""y":${descriptor.y}"""),
            // Verify the descriptor can still compute its FXY correctly
            descriptor.toFXY == expectedFxy
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldUseImplicitDecoderCorrectly =
    test("should use implicit JsonDecoder to decode JSON to DescriptorCode") {
      val testCases = List(
        ("""{"f":0,"x":1,"y":1}""", "001001"),
        ("""{"f":1,"x":1,"y":1}""", "101001"),
        ("""{"f":2,"x":1,"y":1}""", "201001"),
        ("""{"f":3,"x":1,"y":1}""", "301001"),
        // Test different field ordering
        ("""{"y":7,"f":0,"x":1}""", "001007"),
        ("""{"x":31,"y":31,"f":0}""", "031031")
      )
      
      ZIO.foreach(testCases) { case (jsonStr, expectedFxy) =>
        ZIO.succeed {
          // This will use the implicit decoder from the companion object
          val decodedResult = jsonStr.fromJson[DescriptorCode]
          
          assertTrue(
            decodedResult.isRight,
            decodedResult.map(_.toFXY).getOrElse("") == expectedFxy,
            decodedResult.map(_.f).getOrElse(-1) == expectedFxy.head.asDigit
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldImplicitCodecsRoundTripCorrectly =
    test("should round-trip using implicit encoder/decoder") {
      val testCodes = List("001001", "101001", "201001", "301001", "002155", "031002")
      
      ZIO.foreach(testCodes) { fxyCode =>
        ZIO.succeed {
          val original = DescriptorCode.fromFXY(fxyCode).get
          
          // Encode using implicit encoder
          val json = original.toJson
          
          // Decode using implicit decoder
          val decoded = json.fromJson[DescriptorCode]
          
          assertTrue(
            decoded.isRight,
            decoded.map(_ == original).getOrElse(false),
            decoded.map(_.toFXY).getOrElse("") == fxyCode,
            // Verify the methods still work correctly
            decoded.map(_.isElementDescriptor).getOrElse(false) == original.isElementDescriptor,
            decoded.map(_.isReplicationDescriptor).getOrElse(false) == original.isReplicationDescriptor,
            decoded.map(_.isOperatorDescriptor).getOrElse(false) == original.isOperatorDescriptor,
            decoded.map(_.isSequenceDescriptor).getOrElse(false) == original.isSequenceDescriptor
          )
        }
      }.map(_.reduce(_ && _))
    }

  val shouldImplicitDecoderHandleInvalidData =
    test("should use implicit decoder to handle invalid JSON gracefully") {
      val invalidJsonCases = List(
        """{"f":"not_a_number","x":1,"y":1}""",  // Invalid f type
        """{"f":0,"x":"not_a_number","y":1}""",  // Invalid x type  
        """{"f":0,"x":1,"y":"not_a_number"}""",  // Invalid y type
        """{"f":0,"x":1}""",                     // Missing required field y
        """{"f":0,"y":1}""",                     // Missing required field x
        """{"x":1,"y":1}""",                     // Missing required field f
        """{"completely":"different","structure":"here"}""", // Wrong structure
        """not json at all""",                  // Not JSON
        """""",                                 // Empty string
        """null""",                             // Null
        """[]""",                               // Array instead of object
        """{"f":null,"x":1,"y":1}"""            // Null field value
      )
      
      ZIO.succeed {
        assertTrue(
          invalidJsonCases.forall { jsonStr =>
            val result = jsonStr.fromJson[DescriptorCode]
            result.isLeft
          }
        )
      }
    }

  val shouldImplicitCodecsWorkWithCollections =
    test("should work with collections using implicit codecs") {
      ZIO.succeed {
        val descriptors = List(
          DescriptorCode(0, 1, 1),
          DescriptorCode(1, 1, 1), 
          DescriptorCode(2, 1, 1),
          DescriptorCode(3, 1, 1)
        )
        
        // Encode list using implicit encoder
        val json = descriptors.toJson
        
        // Decode list using implicit decoder  
        val decoded = json.fromJson[List[DescriptorCode]]
        
        assertTrue(
          decoded.isRight,
          decoded.map(_.length).getOrElse(0) == 4,
          decoded.map(_ == descriptors).getOrElse(false)
        )
      }
    }

  val shouldImplicitCodecsPreserveDescriptorTypeChecks =
    test("should preserve descriptor type check methods after JSON round-trip") {
      val testCases = List(
        (DescriptorCode(0, 1, 1), "element"),
        (DescriptorCode(1, 1, 1), "replication"),
        (DescriptorCode(2, 1, 1), "operator"),
        (DescriptorCode(3, 1, 1), "sequence")
      )
      
      ZIO.foreach(testCases) { case (original, descriptorType) =>
        ZIO.succeed {
          val json = original.toJson
          val decoded = json.fromJson[DescriptorCode]
          
          val typeCheckResults = decoded.map { desc =>
            descriptorType match {
              case "element" => desc.isElementDescriptor
              case "replication" => desc.isReplicationDescriptor  
              case "operator" => desc.isOperatorDescriptor
              case "sequence" => desc.isSequenceDescriptor
              case _ => false
            }
          }
          
          assertTrue(
            decoded.isRight,
            typeCheckResults.getOrElse(false)
          )
        }
      }.map(_.reduce(_ && _))
    }

  // Test suite focused on the implicit JSON codecs
  val implicitJsonCodecTestSuite = 
    suite("Implicit JSON Codec functionality")(
      shouldUseImplicitEncoderCorrectly,
      shouldUseImplicitDecoderCorrectly,
      shouldImplicitCodecsRoundTripCorrectly,
      shouldImplicitDecoderHandleInvalidData,
      shouldImplicitCodecsWorkWithCollections,
      shouldImplicitCodecsPreserveDescriptorTypeChecks
    )

  val jsonCodecTestSuite = 
    suite("JSON Codec functionality")(
      shouldEncodeDescriptorCodeToJson,
      shouldDecodeJsonToDescriptorCode,
      shouldRoundTripEncodeDecodeCorrectly
    )

  val descriptorCodeFunctionalityTestSuite = 
    suite("DescriptorCode functionality")(
       shouldIdentifyDescriptorTypesCorrectly,
       shouldHandleInvalidFXYCodes,
       applyMethodShouldWorkLikeFromFXY,
       shouldFailWhenNoValidDescriptorCodesFound
    )

  def spec = suite("TableBParserSpec")(
    descriptorCodeFunctionalityTestSuite,
    jsonCodecTestSuite,
    implicitJsonCodecTestSuite
  )    
}