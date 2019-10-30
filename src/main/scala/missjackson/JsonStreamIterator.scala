package missjackson

import java.io.InputStream

import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.util.{Failure, Success, Try}

case class JsonStreamIterator(inputStream: InputStream) extends Iterable[Try[JsonNode]] {
  require(inputStream != null && inputStream.available() > 0, "input stream must not be null or empty")
  private val factory: JsonFactory = new JsonFactory

  override def iterator: Iterator[Try[JsonNode]] = {
    val parser: JsonParser = factory.createParser(inputStream)
    val mapper: ObjectMapper = new ObjectMapper()
    val startElements = Set(START_ARRAY, START_OBJECT)
    val next = () => Try(
      if (startElements.contains(parser.nextToken)) {
        Some(mapper.readValue(parser, classOf[JsonNode]))
      } else {
        None
      })
    val continue: Try[Option[JsonNode]] => Boolean = {
      case Success(None) => false
      case Success(_) => true
      case Failure(_) => false
    }
    val (head, tail) = Iterator.continually(next()).span(continue)
    (head ++ tail.take(1))
      .collect {
        case Success(Some(node)) => Success(node)
        case Failure(t) => Failure(t)
      }
  }

}