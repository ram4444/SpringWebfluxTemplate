package main.kotlin.util

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.springframework.context.annotation.Bean
import java.io.ByteArrayOutputStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory

@Bean
fun string2json(str: String): JsonObject {
    val parser: Parser = Parser()
    val stringBuilder: StringBuilder = StringBuilder(str)
    val json: JsonObject = parser.parse(stringBuilder) as JsonObject
    return json

}

