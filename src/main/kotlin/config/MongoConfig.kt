package main.kotlin.config


import com.mongodb.Mongo
import com.mongodb.MongoClient
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.core.MongoTemplate

class MongoConfig{
    @Bean
    fun mongo():Mongo {
        return MongoClient("172.17.0.2", 27017)
    }

    @Bean
    fun mongoTemplate(): MongoTemplate {
        return MongoTemplate(MongoClient("172.17.0.2", 27017), "test")
    }

}