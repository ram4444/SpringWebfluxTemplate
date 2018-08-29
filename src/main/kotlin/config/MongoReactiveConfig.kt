package main.kotlin.config


import com.mongodb.reactivestreams.client.MongoClient
import com.mongodb.reactivestreams.client.MongoClients
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@EnableReactiveMongoRepositories
class MongoReactiveConfig: AbstractReactiveMongoConfiguration() {

    override fun reactiveMongoClient():MongoClient {
        return MongoClients.create()
    }


    override protected fun getDatabaseName():String {
        return "test";
    }

}