package missjackson

import java.io.InputStream

import com.fasterxml.jackson.core.JsonToken.{START_OBJECT, _}
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.util.{Failure, Try}

case class JsonStreamIterator(inputStream: InputStream) extends Iterable[Try[JsonNode]] {
  require(inputStream != null && inputStream.available() > 0, "input stream must not be null or empty")
  private val factory: JsonFactory = new JsonFactory
  private val mapper: ObjectMapper = new ObjectMapper()
  private val parser: JsonParser = factory.createParser(inputStream)
  private var scanMore = false
  private var token: Try[JsonToken] = {
    val initial = (t: JsonToken) => Set(START_ARRAY, START_OBJECT).contains(t)
    Try(Iterator.iterate(parser.nextToken)(f=>f).find(initial).get)
  }

  val valid: Iterable[JsonNode] = flatMap(_.toOption)

  override def iterator: Iterator[Try[JsonNode]] = new Iterator[Try[JsonNode]] {
    def hasNext: Boolean = token.isSuccess

    def next: Try[JsonNode] = {
      token = positionToStart()
      scanMore = token.toOption.contains(START_OBJECT)
      if (scanMore) {
        Try(mapper.readValue(parser, classOf[JsonNode]))
      } else {
        Failure(new NoSuchElementException)
      }
    }
  }

  private def positionToStart(): Try[JsonToken]  =
    if (!token.toOption.contains(START_OBJECT) || scanMore) {
      Try(parser.nextToken).collect { case t if t != null => t}
    } else {
      token
    }
}