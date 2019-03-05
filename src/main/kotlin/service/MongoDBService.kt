package main.kotlin.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import main.kotlin.repo.TestEntityRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import main.kotlin.pojo.TestEntity
import mu.KotlinLogging

@Service
class MongoDBService {
    val objectMapper = ObjectMapper().registerModule(KotlinModule())
    private val logger = KotlinLogging.logger {}
    @Autowired
    lateinit var repo: TestEntityRepository

    //@Autowired
    //val repository: TestEntityRepository
    //constructor(testEntityRepository:TestEntityRepository) {
        //this.repository = testEntityRepository
    //}

    fun testInsert(): List<TestEntity> {

        logger.debug { "($this.javaClass) is debugging" }
        logger.info { "This is info"}
        val testEntity = TestEntity(null, "tom")

            repo.insert(testEntity)
            //val reactiveTemplate: ReactiveMongoTemplate
            //reactiveTemplate.insert(testEntity)

        return listOf<TestEntity>(testEntity)
    }

    fun testInsert2(name: String): String {

        val testEntity = TestEntity(null, name)

        repo.insert(testEntity)
        //val reactiveTemplate: ReactiveMongoTemplate
        //reactiveTemplate.insert(testEntity)

        return testEntity.id.orEmpty()
    }
}