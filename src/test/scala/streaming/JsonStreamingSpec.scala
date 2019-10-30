package streaming

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import missjackson.JsonStreamIterator
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.collection.JavaConverters._

class JsonStreamingSpec extends WordSpecLike with MustMatchers {

  "When screening results are returned, they" should {
    "be parsed as json values" in {
      val expect =
        """{"header":{"status":"OK","details":"the first results","errorLog":null},
          |"items":[{"id":1,"text":"some secret stuff"},
          |{"id":2,"text":"some secret stuff"}],
          |"pagination":null,"dodgy":true}""".stripMargin.replaceAll("\n","")
      val jitter = JsonStreamIterator(streamFrom("results.json")).valid
      val output = new ByteArrayOutputStream()
      writeToStream(jitter, output)
      new String(output.toByteArray) must be(expect)
    }
  }

  "Multiple documents" should {
    "be merged" in {
      val expect =
        """{"header":{"status":"OK","details":"the first results","errorLog":null},"items":[
          |{"id":1,"text":"some secret stuff"},{"id":2,"text":"some secret stuff"},
          |{"id":3,"text":"some secret stuff"},{"id":4,"text":"some secret stuff"}],
          |"pagination":null,"dodgy":true}""".stripMargin.replaceAll("\n","")
      val allStreams = Seq(streamFrom("results.json"),streamFrom("others.json"))
      val allIter = allStreams.map(s => JsonStreamIterator(s).valid)

      val results = allIter.reduceLeft { (top, item) =>
        val items2 = item
          .map(_.findPath("items"))
          .collect {
            case a: ArrayNode => a
          }
        top.map(_.findPath("items")) foreach {
          case items: ArrayNode =>
            items2.head.elements().asScala foreach(items.add)

          case _ =>
        }
        top
      }

      val output = new ByteArrayOutputStream()
      writeToStream(results, output)
      new String(output.toByteArray) must be(expect)
    }
  }

  "Multiple documents" should {
    "be processed" in {
      def process(responses: Seq[(String, InputStream)]): Iterable[JsonNode] = {
        val results = responses.map {
          case (u,s) =>
            JsonStreamIterator(s).valid
        }
        results.reduceLeft { (top, item) =>
          val items2 = item
            .map(_.findPath("items"))
            .collect {
              case a: ArrayNode => a
            }
          top.map(_.findPath("items")) foreach {
            case items: ArrayNode =>
              items2.head.elements().asScala foreach(items.add)

            case _ =>
          }
          top
        }
      }
      val expect =
        """{"header":{"status":"OK","details":"the first results","errorLog":null},"items":[
          |{"id":1,"text":"some secret stuff"},{"id":2,"text":"some secret stuff"},
          |{"id":3,"text":"some secret stuff"},{"id":4,"text":"some secret stuff"}],
          |"pagination":null,"dodgy":true}""".stripMargin.replaceAll("\n","")
      val inputs = Seq(("a", streamFrom("results.json")),("b",streamFrom("others.json")))
      val results = process(inputs)
      val output = new ByteArrayOutputStream()
      writeToStream(results, output)
      new String(output.toByteArray) must be(expect)
    }
  }

  "Invalid documents" should {
    "be partially processed" in {
      val results = JsonStreamIterator(streamFrom("bad-results.json"))
      results foreach { item =>
        println(s"[$item]")
      }
      true
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