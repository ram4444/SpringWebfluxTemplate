package main.kotlin.pojo.AvroSchema

import org.apache.kafka.common.utils.Bytes
import org.springframework.data.annotation.Id
import java.math.BigDecimal
import java.util.*


data class Node(
        @Id
        val id: String,
        val pinDate: Int, //Default to start of the date
        val pinTS: Int, //The target date
        val pinOriginTS: Int, //The origin of the estimation date, eg the last date of MA series estimate 3 days afterwards
        val timeSerialId: String,
        val createdDate: Int,
        val value: Bytes
    )