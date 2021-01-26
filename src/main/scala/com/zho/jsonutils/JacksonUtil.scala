package com.zho.jsonutils

import java.io.IOException

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode

object JacksonUtil {
  def main(args: Array[String]): Unit = {

    try {
      val objectMapper = new ObjectMapper()
      val str = "{\"data\":{\"birth_day\":7,\"birth_month\":6},\"errcode\":0,\"msg\":\"ok\",\"ret\":0}"
      val data: JsonNode = objectMapper.readTree(str).path("data")
      println(data.get("birth_day").asInt())

    } catch {
      case io: IOException =>
    }
  }


}

class JacksonUtil {

}