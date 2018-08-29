package main.kotlin.repo

import main.kotlin.pojo.TestEntity
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository


interface TestEntityRepository : MongoRepository<TestEntity, String> {
    //companion object {

    //}
    //val nickname: String
    //init {
        //nickname = _nickname
    //}
    //fun findAllByValue(value: String): Flux<TestEntity>
    //fun findOne(owner: Mono<String>): Mono<TestEntity>
}

