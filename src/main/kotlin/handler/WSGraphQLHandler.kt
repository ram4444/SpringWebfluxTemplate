package main.kotlin.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import graphql.ExecutionResult
import graphql.schema.DataFetcher
import graphql.schema.StaticDataFetcher
import main.kotlin.graphql.GraphQLHandler
import main.kotlin.graphql.GraphQLRequest
import main.kotlin.reactiverepo.TestEntityReactRepository
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import main.kotlin.service.MongoDBReactiveService
import main.kotlin.service.WebfluxService
import org.springframework.beans.factory.annotation.Autowired
import javax.annotation.PostConstruct
import com.fasterxml.jackson.core.JsonProcessingException
import graphql.execution.reactive.CompletionStageMappingPublisher
import main.kotlin.config.WebSocketConfig
import main.kotlin.pojo.TestEntity
import main.kotlin.repo.TestEntityRepository
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.web.reactive.socket.WebSocketMessage
import reactor.core.publisher.*
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer


@Component
class WSGraphQLHandler : WebSocketHandler {
    private val topicprocessor = TopicProcessor.share<String>("shared", 1024)

    private val subscriptionRef = AtomicReference<Subscription>()

    val objectMapper = ObjectMapper().registerModule(KotlinModule())

    @Autowired
    val mongoDBservice: MongoDBReactiveService = MongoDBReactiveService()
    @Autowired
    lateinit var repo: TestEntityReactRepository
    @Autowired
    val webFluxDBService: WebfluxService = WebfluxService()
    //Initiate schema from somewhere
    val schema ="""
            type Query{
                query_funca: Int
            }
            type Subscription{
                query_func1: Int
                query_func2: TestEntity
            }
            type TestEntity{
                id: String
                name: String
            }
            schema {
              query: Query
              subscription: Subscription
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
    lateinit var handler: GraphQLHandler
    var testrtn : Flux<Int> = Flux.just(42).publish()

    @PostConstruct
    fun init() {
        //--DELETE WHEN NOT IN USE--
        /*Initialize sample Object
        val sample_obj1:SampleEntity = SampleEntity("1")
        val sample_obj2:SampleEntity = SampleEntity("2")
        */

        //initialize Fetchers
        fetchers = mapOf(
                "Subscription" to
                        listOf(
                                "query_func1" to StaticDataFetcher(testrtn),
                                "query_func2" to DataFetcher{repo.findAll()}

                        )
        )

        handler = GraphQLHandler(schema, fetchers)
    }
    @Scheduled(fixedRate = 5000)

     //Example from Bael
    override fun handle(session: WebSocketSession): Mono<Void> {


        //{"query": "subscription{query_func2{id,name}}","params":{"what":"env"}, "operationName":""}
         //mapOf("data" to result.getData<Any>())

         //session.send (Publisher<WebsocketMessage>) ----MonoVoid
         //session.textMessage("") ------- WebsocketMessage

         //topicprocessor.map{ ev -> session.textMessage("AAAA") } -- Flux<WebSocketMessage>
         //result.getData<Any>() -- Flux<Entity>
         //session.receive() -- Flux<WebSocketMessage>
         //session.receive().map{ev -> type} --Flux<type>
        return session.send(topicprocessor.map { ev -> session.textMessage("Here only return for 1 time") })
                .and(session.receive().map{ ev ->
                    val json = ev.payloadAsText
                    val graphQLRequest:GraphQLRequest= objectMapper.readValue(json, GraphQLRequest::class.java)
                    val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx=null)
                    val resultStream: Publisher<ExecutionResult>  = result.getData()
                    class OvrSubscriber:Subscriber<ExecutionResult>{
                        override fun onSubscribe(s:Subscription) {
                            println("subscribe")
                            subscriptionRef.set(s)
                            request(1)
                        }
                        override fun onNext(graphqlExeResult:ExecutionResult) {
                            //println("updating...")
                            val stockPriceUpdate = graphqlExeResult.getData<Any>().toString()
                            println(graphqlExeResult.getData<Any>().toString())
                            session.send(topicprocessor.map { ev -> session.textMessage(stockPriceUpdate) })
                            request(1)
                        }
                        override fun onError(t: Throwable) {
                            println("error")
                            session.close()
                        }
                        override fun onComplete() {
                            println("completed")
                            session.close()
                        }
                    }
                    resultStream.subscribe(OvrSubscriber() )

                }.log().map { objectMapper.writeValueAsString(it) }
                        .doOnNext { ev -> topicprocessor.onNext(ev) })


         /*
         val publisher = session.receive().map{ ev ->
             val json = ev.payloadAsText
             val graphQLRequest:GraphQLRequest= objectMapper.readValue(json, GraphQLRequest::class.java)
             val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx=null)
             //val websocketMsg: WebSocketMessage = WebSocketMessage(WebSocketMessage.Type.TEXT,"")
             mapOf("data" to result.getData<Any>()).toString()
             result
         }
         */
         /*
         return session.send(processor
                 .map { ev -> session.textMessage("MMMM") }).log().doOnNext{ ev -> processor.onNext("111") }
                 */
    }

    /*
    suspend fun getResultGraphQL(session: WebSocketSession) {
        val mRet = session.receive().map{ ev ->
            val json = ev.payloadAsText
            val graphQLRequest:GraphQLRequest= objectMapper.readValue(json, GraphQLRequest::class.java)
            val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx=null)
            result.getData<WebSocketMessage>()
        }
    }
    */

    private fun request(n: Int) {
        val subscription = subscriptionRef.get()
        subscription?.request(n.toLong())
    }


}