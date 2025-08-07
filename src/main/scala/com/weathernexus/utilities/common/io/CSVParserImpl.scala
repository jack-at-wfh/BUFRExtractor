package com.weathernexus.utilities.common.io

import zio._

final case class CSVParserImpl() extends CSVParser {
  override def parseLine(line: String): Array[String] = {

    if (line.isEmpty) return Array("")

    sealed trait ParserState
    case object InitialField extends ParserState
    case object InUnquotedField extends ParserState
    case object InQuotedField extends ParserState
    case object AfterClosingQuote extends ParserState

    @scala.annotation.tailrec
    def loop(
        idx: Int,
        currentFieldChars: List[Char],
        state: ParserState,
        completedFields: List[String]
    ): List[String] = {

      def finaliseField(chars: List[Char], finalState: ParserState): String = {
        finalState match {
          case InQuotedField | AfterClosingQuote => chars.mkString
          case _                                 => chars.mkString.trim
        }
      }

      def finaliseAndReset(chars: List[Char], currentState: ParserState): (String, List[Char], ParserState) = {
        val field = finaliseField(chars, currentState)
        (field, List.empty[Char], InitialField)
      }

      if (idx >= line.length) {
        val finalField = finaliseField(currentFieldChars, state)
        (finalField :: completedFields).reverse
      } else {
        val char = line.charAt(idx)

        state match {
          case InitialField =>
            char match {
              case ' ' | '\t' =>
                loop(idx + 1, currentFieldChars, InitialField, completedFields)
              case '"' =>
                loop(idx + 1, List.empty[Char], InQuotedField, completedFields)
              case ',' =>
                val (field, _, _) = finaliseAndReset(currentFieldChars, InitialField)
                loop(idx + 1, List.empty[Char], InitialField, field :: completedFields)
              case _ =>
                loop(idx + 1, currentFieldChars :+ char, InUnquotedField, completedFields)
            }

          case InUnquotedField =>
            char match {
              case '"' =>
                loop(idx + 1, currentFieldChars :+ char, InUnquotedField, completedFields)
              case ',' =>
                val (field, newChars, newState) = finaliseAndReset(currentFieldChars, InUnquotedField)
                loop(idx + 1, newChars, newState, field :: completedFields)
              case _ =>
                loop(idx + 1, currentFieldChars :+ char, InUnquotedField, completedFields)
            }

          case InQuotedField =>
            char match {
              case '"' =>
                if (idx + 1 < line.length && line.charAt(idx + 1) == '"') {
                  loop(idx + 2, currentFieldChars :+ '"', InQuotedField, completedFields)
                } else {
                  loop(idx + 1, currentFieldChars, AfterClosingQuote, completedFields)
                }
              case _ =>
                loop(idx + 1, currentFieldChars :+ char, InQuotedField, completedFields)
            }

          case AfterClosingQuote =>
            char match {
              case ' ' | '\t' =>
                loop(idx + 1, currentFieldChars, AfterClosingQuote, completedFields)
              case ',' =>
                val (field, newChars, newState) = finaliseAndReset(currentFieldChars, AfterClosingQuote)
                loop(idx + 1, newChars, newState, field :: completedFields)
              case _ =>
                loop(idx + 1, currentFieldChars :+ '"' :+ char, InUnquotedField, completedFields)
            }
        }
      }
    }

    loop(0, List.empty[Char], InitialField, List.empty[String]).toArray
  }
}