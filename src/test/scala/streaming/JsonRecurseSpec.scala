package streaming

import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.{MustMatchers, WordSpecLike}

class JsonRecurseSpec extends WordSpecLike with MustMatchers {

  private val starters = Set(START_ARRAY,START_OBJECT)
  private val closers = Set(END_ARRAY,END_OBJECT)
  "A json document" should {
    "be recursively parsed" in {
      val factory: JsonFactory = new JsonFactory
      val mapper: ObjectMapper = new ObjectMapper()
      val inputStream = streamFrom("array.json")

      val parser: JsonParser = factory.createParser(inputStream)
      val parse = () => {
        val next = parser.nextToken
        if(starters.contains(next)) {
          Right(mapper.readValue(parser, classOf[JsonNode]))
        } else {
          Left(parser.nextToken)
        }
      }

      val iterator = Iterator.continually(parse()).takeWhile {
        case Left(END_ARRAY | END_OBJECT) => false
        case Left(null) => false
        case _ => true
      }
      val result: Iterator[JsonNode] = iterator.collect { case Right(t) => t}
      result.foreach(println)
    }
  }

  private def streamFrom(resource: String) = {
    getClass.getResourceAsStream(s"/$resource")
  }


}