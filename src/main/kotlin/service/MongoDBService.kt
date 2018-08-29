package main.kotlin.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import main.kotlin.repo.TestEntityRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import main.kotlin.pojo.TestEntity

@Service
class MongoDBService {
    val objectMapper = ObjectMapper().registerModule(KotlinModule())
    @Autowired
    lateinit var repo: TestEntityRepository

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
}