package main.kotlin.pojo

import lombok.Getter
import lombok.Setter
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
@Getter
@Setter
@Document(collection="TestEntity")
data class TestEntity(
        @Id
        val id: String?,
        //val is final/const
        var name: String
    )