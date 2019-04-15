package main.kotlin.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import main.kotlin.reactiverepo.TestEntityReactRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import main.kotlin.pojo.TestEntity

@Service
class MongoDBReactiveService {
    val objectMapper = ObjectMapper().registerModule(KotlinModule())
    @Autowired
    lateinit var repo: TestEntityReactRepository

    //@Autowired
    //val repository: TestEntityRepository
    //constructor(testEntityRepository:TestEntityRepository) {
        //this.repository = testEntityRepository
    //}

    fun testInsert(): List<TestEntity> {

        val testEntity = TestEntity(null, "tom")

            repo.insert(testEntity)
            //val reactiveTemplate: ReactiveMongoTemplate
            //reactiveTemplate.insert(testEntity)

        return listOf<TestEntity>(testEntity)
    }

    //TODO: Get a list for watching stocks

    //TODO: Insert record of querying historic data

    // TODO: Get record from record of querying historic data (To prevent multiple calling)

    // TODO: Insert record for time series node

    // TODO: Get a list of time series node by some criteria

    //
}