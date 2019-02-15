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
import kotlin.coroutines.experimental.buildIterator


@Component
class WSGraphQLHandler : WebSocketHandler {
    private val topicprocessor = TopicProcessor.share<Void>("shared", 1024)
    private val emitterProcessor = EmitterProcessor.create<String>()
    //private val subscriptionRef = AtomicReference<Subscription>()

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
                query_func2: Int
            }
            type TestEntity{
                id: String
                name: String
            }
            schema {
              query: Query
              subscription: Subscription
            }"""

    lateinit var fetchers: Map<String, List<Pair<String, DataFetcher<out Any>>>>
    lateinit var handler: GraphQLHandler
    var testrtn : Flux<String> = Flux.just("abcde").publish()

    @PostConstruct
    fun init() {

        //initialize Fetchers
        fetchers = mapOf(
                "Subscription" to
                        listOf(
                                "query_func1" to StaticDataFetcher(testrtn),
                                "query_func2" to DataFetcher{repo.count().repeat()}

                        )
        )

        handler = GraphQLHandler(schema, fetchers)
    }

     //Example from Bael
    override fun handle(session: WebSocketSession): Mono<Void> {
         println("session detected")

         //session.send(Publisher/Flux<WebsocketMessage>) is a mono<Void>
         // result.getData() is a Mono<Any>/Flux<Any>
         //session.receive() is Flux<WebsocketMessage>
         //session.receive().flatMap { it -> it.payloadAsText}
         //session.send(Flux.interval(Duration.ofSeconds(1)).map {n -> n.toString()}.map {session.textMessage("rtn")  })

         /*
         val graphQLFlux =

                 session.receive().map { ev ->
                     val json = ev.payloadAsText
                     val graphQLRequest: GraphQLRequest = objectMapper.readValue(json, GraphQLRequest::class.java)
                     val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx = null)
                     val resultStream: Mono<String> = result.getData()
                     resultStream

                 }.flatMap { it -> it.toMono() }
        */


         /* This print 123456 only
        val rtn = session.send(Flux.interval(Duration.ofSeconds(1)).map {n -> n.toString()}.mergeWith(graphQLFlux).map {
            it ->
            println(it)
            session.textMessage(it)
        })
        */
         //println(session.receive().toString())

         /* This return a stream of const msg
         val rtn = session.send(
                    Flux.interval(Duration.ofSeconds(1))
                    .map{n -> n.toString()}
                    .map {session.textMessage("rtn")  }
                    )
        */



         return session.send(topicprocessor.map { ev -> session.textMessage("Subcribed") }).and(session.receive().map { ev ->
             val json = ev.payloadAsText
             val graphQLRequest: GraphQLRequest = objectMapper.readValue(json, GraphQLRequest::class.java)
             val result = handler.execute_subscription(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx = null)
             val resultStream: Publisher<ExecutionResult> = result.getData()
             val subscriptionRef = AtomicReference<Subscription>()
             class OvrSubscriber:Subscriber<ExecutionResult> {

                 override fun onSubscribe(s: Subscription) {
                     println("subscribe")
                     subscriptionRef.set(s);
                     s.request(1);
                 }

                 override fun onNext(er: ExecutionResult ) {
                     //
                     // process the next stock price
                     //
                     //processStockPriceChange(er.getData());

                     //
                     // ask the publisher for one more item please
                     //
                     println(er.getData<ExecutionResult>())
                     session.send(topicprocessor.map { ev -> session.textMessage(er.getData<ExecutionResult>().toString()) })
                     subscriptionRef.get().request(1)
                 }

                 override fun onError(t: Throwable) {
                     //
                     // The upstream publishing data source has encountered an error
                     // and the subscription is now terminated.  Real production code needs
                     // to decide on a error handling strategy.
                     //
                     println("error")
                     session.close()
                 }

                 override fun onComplete() {
                     //
                     // the subscription has completed.  There is not more data
                     //
                     println("completed")
                     session.close()
                 }
             }
             resultStream.subscribe(OvrSubscriber())
         }).log()
                 .doOnNext { ev -> topicprocessor.onNext(ev) }

    }

}