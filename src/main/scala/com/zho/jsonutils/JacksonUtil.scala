package com.zho.jsonutils

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.codehaus.jackson.map.ObjectMapper

import scala.beans.BeanProperty

object JacksonUtil {
  def main(args: Array[String]): Unit = {
    // https://www.cnblogs.com/lone5wolf/p/10940869.html
    try {
      val json = "{\"name\":\"小民\",\"age\":20,\"birthday\":\"844099200000\",\"email\":\"xiaomin@sina.com\"}"
      val mapper = new ObjectMapper()
      //    mapper.registerModule(DefaultScalaModule)

      val value: Any = mapper.readValue(json, classOf[JacksonUtils])
      println(value)
    }
  }


}

case class JacksonUtils(
                        @BeanProperty var name: String = null,
                        @BeanProperty var age: Integer = null,
                        @BeanProperty var birthday: String = null,
                        @BeanProperty var email: String = null
                      )