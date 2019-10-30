package streaming

import java.io.{ByteArrayOutputStream, OutputStream}

import com.fasterxml.jackson.databind.JsonNode
import missjackson.JsonStreamIterator
import org.scalatest.{MustMatchers, OptionValues, WordSpecLike}

import scala.util.{Failure, Success}

class JsonStreamingSpec extends WordSpecLike with MustMatchers with OptionValues  {

  "When streaming results are returned, they" should {
    "be parsed as json values" in {
      val expect =
        """{"header":{"status":"OK","details":"the first results","errorLog":null},
          |"items":[{"id":1,"text":"some secret stuff"},
          |{"id":2,"text":"some secret stuff"}],
          |"pagination":null,"dodgy":true}""".stripMargin.replaceAll("\n","")
      val jitter = JsonStreamIterator(streamFrom("results.json"))
      val output = new ByteArrayOutputStream()
      writeToStream(jitter.collect { case Success(t) => t}, output)
      new String(output.toByteArray) must be(expect)
    }
  }

  "When screening results are returned, they" should {
    "be parsed as json values" in {
      val expect =
        """{"requestedUrl":"https://www.googleapis.com/books/v1/volumes?q=java&maxResults=40",
          |"items":[{"kind":"books#volume","id":"7tkN1CYzn2cC","etag":"pfjjxSpetIM",
          |"selfLink":"https://www.googleapis.com/books/v1/volumes/7tkN1CYzn2cC",
          |"volumeInfo":{"title":"A Hypervista of the Java Landscape","publisher":"InfoStrategist.com",
          |"industryIdentifiers":[{"type":"ISBN_13","identifier":"9781592432172"}
          |,{"type":"ISBN_10","identifier":"1592432174"}]}}]}""".stripMargin.replaceAll("\n","")
      val jitter = JsonStreamIterator(streamFrom("sample.json"))
      val output = new ByteArrayOutputStream()
      writeToStream(jitter.collect { case Success(t) => t}, output)
      new String(output.toByteArray) must be(expect)
    }
  }

  "Invalid documents" should {
    "be detected" in {
      val iterator = JsonStreamIterator(streamFrom("bad-results.json"))
      val results = iterator.collectFirst { case Failure(t) => t }
      results.value.getMessage must be(
        """Unexpected character ('"' (code 34)): was expecting comma to separate Object entries
          | at [Source: (BufferedInputStream); line: 16, column: 6]""".stripMargin)
    }
  }

  "when a null input stream is provided, the streamer" should {
    "note this and terminate" in {
      val thrown = intercept[Exception] {
        JsonStreamIterator(null)
      }
      thrown.getMessage must be("requirement failed: input stream must not be null or empty")
    }
  }

  private def streamFrom(resource: String) = {
    getClass.getResourceAsStream(s"/$resource")
  }

  private  def writeToStream(iterator: Iterable[JsonNode], outputStream: OutputStream): Unit = {
    iterator.foreach(item => outputStream.write(item.toString.getBytes()) )
  }

}