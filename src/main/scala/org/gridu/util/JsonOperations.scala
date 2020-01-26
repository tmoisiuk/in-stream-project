package org.gridu.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

  object JsonOperations {

  private val mapper: ObjectMapper with ScalaObjectMapper = JsonOperations.objectMapper

  implicit class Serializer[T](obj: T) {
    def toJson: String = mapper.writeValueAsString(obj)
  }

  implicit class Deserializer(json: String) {
    def as[A](implicit m: Manifest[A]): A =
      try {
        mapper.readValue[A](json)
      } catch {
        case _: Exception =>
          val className = m.runtimeClass.getSimpleName
          throw new Exception(s"""Can not cast "$json" to $className""")
      }
  }

  private def objectMapper: ObjectMapper with ScalaObjectMapper = {

    val objectMapper: ObjectMapper with ScalaObjectMapper = new ObjectMapper() with ScalaObjectMapper

    objectMapper
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .setSerializationInclusion(Include.NON_NULL)
      .enable(SerializationFeature.INDENT_OUTPUT)

    objectMapper
  }
}
