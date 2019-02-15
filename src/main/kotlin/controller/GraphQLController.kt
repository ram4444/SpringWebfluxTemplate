package main.kotlin.controller

import com.beust.klaxon.JsonArray
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Klaxon
import com.beust.klaxon.Parser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.JSONPObject
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.kittinunf.fuel.core.Request
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.httpPost
import com.github.kittinunf.result.Result
import graphql.schema.DataFetcher
import graphql.schema.StaticDataFetcher
import main.kotlin.graphql.GraphQLHandler
import main.kotlin.graphql.GraphQLRequest
import main.kotlin.pojo.LightComment
import main.kotlin.pojo.Response
import main.kotlin.pojo.TestEntity
import main.kotlin.service.MongoDBService
import main.kotlin.service.WebfluxJSONPlaceholderService
import main.kotlin.service.WebfluxService
import main.kotlin.util.string2json
import org.springframework.http.MediaType.*
import org.springframework.http.ResponseEntity
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import pl.wendigo.chrome.ChromeProtocol
import pl.wendigo.chrome.InspectablePage
import pl.wendigo.chrome.Inspector
import pl.wendigo.chrome.domain.page.NavigateRequest
import pl.wendigo.chrome.HeadlessSession


import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers
import javax.annotation.PostConstruct

@RestController
class GraphQLController() {

    @Autowired
    val mongoDBservice: MongoDBService = MongoDBService()
    @Autowired
    val webFluxDBService: WebfluxService = WebfluxService()
    @Autowired
    val webfluxJSONPlaceholderService: WebfluxJSONPlaceholderService = WebfluxJSONPlaceholderService()

    //Initiate schema from somewhere
    val schema ="""
            type Query{
                query_func1: Int
                query_func2: [TestEntity]
                query_func3(name: String!): String!
            }
            type TestEntity{
                id: String
                name: String
            }"""

    lateinit var fetchers: Map<String, List<Pair<String, DataFetcher<out Any>>>>
    lateinit var handler:GraphQLHandler

    @PostConstruct
    fun init() {

        //initialize Fetchers
        fetchers = mapOf(
                "Query" to
                        listOf(
                                "query_func1" to StaticDataFetcher(999),
                                "query_func2" to DataFetcher{mongoDBservice.testInsert()},
                                "query_func3" to DataFetcher{mongoDBservice.testInsert2(it.getArgument("name"))}

                        )
        )

        handler = GraphQLHandler(schema, fetchers)
    }

    @RequestMapping("/")
    suspend fun pingcheck():String {
        println("ping")
        return "success"
    }

    @PostMapping("/graphql")
    fun executeGraphQL(@RequestBody request:GraphQLRequest):Map<String, Any> {

        val result = handler.execute(request.query, request.params, request.operationName, ctx = null)

        return mapOf("data" to result.getData<Any>())
    }

    @PostMapping("/webflux")
    fun getData(@RequestBody name:String): Mono<ResponseEntity<List<TestEntity>>> {
        return webFluxDBService.fetchTestEntity(name)
                //.filter { it -> it. % 2 == 0 } // can be obmitted
                .take(20) // can be obmitted
                .parallel(4) // can be obmitted
                .runOn(Schedulers.parallel())
                // insert any mapping of Flux<Entity> here
                .sequential()
                .collectList()
                .map { body -> ResponseEntity.ok().body(body) }
                .toMono()
    }

    //Sample from JSONPlaceholder
    @PostMapping("/webfluxtest")
    fun getDataFromPlaceholder(): Mono<ResponseEntity<List<Response>>> {
        return webfluxJSONPlaceholderService.fetchPosts()
                .filter { it -> it.userId % 2 == 0 }
                .take(20)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map { post -> webfluxJSONPlaceholderService.fetchComments(post.id)
                        .map { comment -> LightComment(email = comment.email, body = comment.body) }
                        .collectList()
                        .zipWith(post.toMono()) }

                .flatMap { it -> it }
                .map { result -> Response(
                        postId = result.t2.id,
                        userId = result.t2.userId,
                        title = result.t2.title,
                        comments = result.t1
                ) }
                .sequential()
                .collectList()
                .map { body -> ResponseEntity.ok().body(body) }
                .toMono()
    }

}
