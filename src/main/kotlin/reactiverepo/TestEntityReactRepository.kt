package main.kotlin.reactiverepo

import main.kotlin.pojo.TestEntity
import org.bson.types.ObjectId
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.stereotype.Repository


interface TestEntityReactRepository : ReactiveMongoRepository<TestEntity, String> {
    //companion object {

    //}
    //val nickname: String
    //init {
        //nickname = _nickname
    //}
    //fun findAllByValue(value: String): Flux<TestEntity>
    //fun findOne(owner: Mono<String>): Mono<TestEntity>
}

