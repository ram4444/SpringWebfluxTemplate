package main.kotlin.pojo.MongoSchema

import lombok.Getter
import lombok.Setter
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.math.BigDecimal
import java.util.*

@Getter
@Setter
@Document(collection="Node")
data class Node(
        @Id
        val id: ObjectId?,
        val pinDate: Date,
        val timeSerialId: ObjectId,
        val createdDate: Date,
        val value: BigDecimal
    )