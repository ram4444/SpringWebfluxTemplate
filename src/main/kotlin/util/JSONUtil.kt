package main.kotlin.util

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.springframework.context.annotation.Bean

@Bean
fun string2json(str: String): JsonObject {
    val parser: Parser = Parser()
    val stringBuilder: StringBuilder = StringBuilder(str)
    val json: JsonObject = parser.parse(stringBuilder) as JsonObject
    return json
}