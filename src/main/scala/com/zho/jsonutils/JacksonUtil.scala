package com.zho.jsonutils

import java.io.IOException
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode

object JacksonUtil {
  def main(args: Array[String]): Unit = {

    try {
      val objectMapper = new ObjectMapper()
      val str =
        """
          |{"properties":{"DATE":{"type":"text", "fields":{"keyword":{"ignore_above":"256", "type":"keyword"}}}, "CASEFOLDERID":{"type":"keyword"}}}
          |""".stripMargin
      val data: JsonNode = objectMapper.readTree(str).path("properties")

      import scala.collection.JavaConverters._
//      val value: util.Iterator[String] = data.fieldNames()
      println(data.fieldNames().asScala.toList)

    } catch {
      case io: IOException => println(io)
    }
  }


}

class JacksonUtil {

}