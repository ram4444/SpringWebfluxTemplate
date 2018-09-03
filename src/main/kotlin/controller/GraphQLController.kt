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
            }
            type TestEntity{
                id: String
                name: String
            }"""
    /* Schema Example
    val schema ="""
            type Query{
                answer: Int
                hello(what:String="World"):String
                testEntityList: [TestEntity]
            }
            type TestEntity{
                id: String
                name: String
            }
            type Mutation{
                sosad: Int
                test: String
            }
        """
    */

    lateinit var fetchers: Map<String, List<Pair<String, DataFetcher<out Any>>>>
    lateinit var handler:GraphQLHandler

    @PostConstruct
    fun init() {
        //--DELETE WHEN NOT IN USE--
        /*Initialize sample Object
        val sample_obj1:SampleEntity = SampleEntity("1")
        val sample_obj2:SampleEntity = SampleEntity("2")
        */

        //initialize Fetchers
        fetchers = mapOf(
                "Query" to
                        listOf(
                                "query_func1" to StaticDataFetcher(999),
                                "query_func2" to DataFetcher{mongoDBservice.testInsert()}

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
        //For Simple Case
        //curl 127.0.0.1:8080/graphql -H content-type:application/json -d'{"query": "query{hello,answer,testEntity{name,id}}","params":{"what":"env"}}'
        // For JSON Array
        //curl 127.0.0.1:8080/graphql -H content-type:application/json -d'{"query": "query{hello,answer,testEntityList{name,id}}","params":{"what":"env"}}'
        //curl 127.0.0.1:8080/graphql -H content-type:application/json -d'{"query": "{query_func2{id,name}}","params":{"what":"env"}, "operationName":""}'
        //curl 127.0.0.1:8080/graphql -H content-type:application/json -d'{"query": "mutation{sosad}","params":{"what":"env"}}'
        //curl 127.0.0.1:8080/graphql -H content-type:application/json -d'{"query": "{query_func1}","params":{"what":"env"}}'

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


    /* Example of incoming RequestBody json
        {
            "header" : [
                {"Authorization" : "Bearer token"},
                {}
            ],
            "body" : [
                {"data-type": "raw", "key": "grant_type", "value": "password"},
                {"data-type": "raw", "json": "json"
            ]
        }
    */
    @PostMapping("/curlasyncbyfuel")
    fun curlAsyncByfuel(@RequestHeader method:String, @RequestHeader url:String, @RequestBody requestBody:String):Map<String, Any> {
        var rtn:String=""

        println("---------------------------------------------------------")
        println("method: "+method)
        println("body:")
        println(requestBody)

        if (method.toLowerCase().equals("get")){
            url.httpGet().responseString { request, response, result ->

                when (result) {
                    is Result.Failure -> {
                        val ex = result.getException()
                        println("exception: "+ ex)
                        println("response: "+ response)
                        rtn = ex.toString()
                    }
                    is Result.Success -> {
                        val data = result.get()
                        println("data: "+ data)
                        println("response: "+ response)
                        rtn = data.toString()
                    }
                }
            }
        } else if (method.toLowerCase().equals("post")) {
            url.httpPost().responseString { request, response, result ->
                //do something with response\

                when (result) {
                    is Result.Failure -> {
                        val ex = result.getException()
                        println("exception: "+ ex)
                        println("response: "+ response)
                        rtn = ex.toString()
                    }
                    is Result.Success -> {
                        val data = result.get()
                        println("data: "+ data)
                        println("response: "+ response)
                        rtn = data.toString()
                    }
                }
            }
        } else {
            rtn = "Other method is not support"
        }
        return mapOf("data" to rtn)
    }

    @PostMapping("/curlbyfuel")
    fun curlByfuel(@RequestHeader method:String, @RequestHeader url:String, @RequestBody body: String):Map<String, Any> {
        //JSON is in the @RequestBody
        var rtn:String=""

        /*
        println("---------------------------------------------------------")
        println("method: "+method)
        println("body:")
        println(body)
        */

        //val objectMapper = ObjectMapper().registerModule(KotlinModule())
        val bodymap: Map<String, Map<String,Any>> = jacksonObjectMapper().readValue(body.trim())
        //val body = string2json(body)

        if (method.toLowerCase().equals("get")){
            // Get the header(to be sent) from the body

            //JsonReader(StringReader(array))
            val fuelcurlHeader_map:Map<String,Any>? = bodymap.get("header")
            //val fuelcurlHeader = body.get("header") as JsonObject

            if (null != fuelcurlHeader_map) {
                // Header does exist
                //val fuelcurlHeader:List<Map<String, String>> = jacksonObjectMapper().readValue(fuelcurlHeader_listmap)
                //val headermap:Map<String, Any>? = mapOf(bodymap.get("header"))
                val (request, response, result) = url.httpGet().header(fuelcurlHeader_map).responseString()
                println("----------------request: ")
                println(request)
                println("-------------------------")
                return mapOf("rtn_code" to response.statusCode,
                            "response" to response.responseMessage,
                            "result" to result.get()
                )
            } else {
                // Header does not exist
                val (request, response, result) = url.httpGet().responseString()
                return mapOf("rtn_code" to "success", "response" to response.data, "result" to result.get())
            }

        } else if (method.toLowerCase().equals("post")) {
            /* Post method
            * */
            // Get the header and body (to be sent) from the body
            val fuelcurlHeader_map:Map<String,Any>? = bodymap.get("header")
            val fuelcurlBody_map:Map<String,Any>? = bodymap.get("body")

            if (null != fuelcurlBody_map) {
                //val listpairStringAny: List<Pair<String, Any>> = jacksonObjectMapper().readValue(fuelcurlBody_str)
                val fuelcurlBody_listpairstrany:List<Pair<String,Any?>>? = fuelcurlBody_map.toList()
                val (request, response, result) = url.httpPost(fuelcurlBody_listpairstrany).responseString()
                println("----------------request: ")
                println(request)
                println("-------------------------")
                return mapOf("rtn_code" to response.statusCode,
                        "response" to response.responseMessage,
                        "result" to result.get()
                )
            } else {
                val (request, response, result) = url.httpPost().responseString()
                println("----------------request: ")
                println(request)
                println("-------------------------")
                return mapOf("rtn_code" to response.statusCode,
                        "response" to response.responseMessage,
                        "result" to result.get()
                )
            }
        } else {
            return mapOf("rtn_code" to "fail", "msg" to "Other methmod is not allow")
        }
    }


}
