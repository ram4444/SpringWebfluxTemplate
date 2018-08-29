package main.kotlin.config

import com.fasterxml.jackson.databind.ObjectMapper
import graphql.schema.DataFetcher
import graphql.schema.StaticDataFetcher
import main.kotlin.graphql.GraphQLHandler
import main.kotlin.graphql.GraphQLRequest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import main.kotlin.handler.PrimeNumbersHandler
import main.kotlin.handler.WSGraphQLHandler
import main.kotlin.reactiverepo.TestEntityReactRepository
import main.kotlin.service.WebfluxService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.channel.PublishSubscribeChannel
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.file.dsl.Files
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.ConnectableFlux
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer


@Configuration
@EnableWebFlux
@ComponentScan(value = ["org.zupzup.kotlinwebfluxdemo"])
class WebSocketConfig(
        val primeNumbersHandler: PrimeNumbersHandler,
        val graphQLHandler: WSGraphQLHandler
) {
    @Bean
    fun websocketHandlerAdapter() = WebSocketHandlerAdapter()

    @Bean
    fun handlerMapping() : HandlerMapping {
        val handlerMapping = SimpleUrlHandlerMapping()
        handlerMapping.urlMap = mapOf(
                "/ws/primes" to primeNumbersHandler,
                "/ws/graphql" to graphQLHandler,
                "/ws/graphql2" to wsh()
        )
        handlerMapping.order = 1
        return handlerMapping
    }

    @Bean
    //Listener of file create in a directory
    fun incomingFileFlow (@Value("file://\${HOME}/Desktop/in") f: File) =
            IntegrationFlows.from(Files.inboundAdapter(f).autoCreateDirectory(true),
                    { p->p.poller(
                            { pm->pm.fixedRate(1000) })
                    }).channel(incomingChannel()).get()

    @Bean
    fun incomingChannel() = PublishSubscribeChannel()
    //Example from https://www.youtube.com/watch?v=GlvyHIqT3K4

    //-------------------------------------------------------------------------------
    @Autowired
    lateinit var repo: TestEntityReactRepository
    @Autowired
    val webFluxDBService: WebfluxService = WebfluxService()

    var testrtn : Flux<Int> = Flux.just(42)
    //Initiate schema from somewhere
    val schema ="""
            type Query{
                query_funca: Int
            }
            type Subscription{
                query_func1: Int
                query_func2: [TestEntity]
            }
            type TestEntity{
                id: String
                name: String
            }"""

    //initialize Fetchers
    val fetchers = mapOf(
        "Subscription" to
        listOf(
        "query_func1" to StaticDataFetcher(testrtn),
        "query_func2" to DataFetcher{repo.findAll()}

        )
    )

    val handler: GraphQLHandler = GraphQLHandler(schema, fetchers)

    /*
    @Bean
    fun graphQLSubscript(session: WebSocketSession, sink: FluxSink<WebSocketMessage>) {
        while(true) {
            var mapresult = session.receive().map { ev ->
                val objectMapper = ObjectMapper()
                val json = ev.payloadAsText
                val graphQLRequest: GraphQLRequest = objectMapper.readValue(json, GraphQLRequest::class.java)
                val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx = null)
                mapOf("data" to result.getData<Any>())
            }

            sink.next(session.textMessage(mapresult.toString()))
        }
    }
    */
    //--------------------------------------------------------------

    @Bean
    fun wsh(): WebSocketHandler {
        val om= ObjectMapper()
        val connections = ConcurrentHashMap<String, MessageHandler>()

        class ForwardingMessageHandler(val session: WebSocketSession, val sink: FluxSink<WebSocketMessage>) : MessageHandler {
            private val sessionId = session.id
            override fun handleMessage(msg: Message<*>) {

                // sample from https://www.youtube.com/watch?v=GlvyHIqT3K4

                val payload = msg.payload as File
                val fe = FileEvent(sessionId = sessionId, path= payload.absolutePath)
                val str = om.writeValueAsString(fe)
                val tm = session.textMessage(str)
                sink.next(tm)

                /*
                session.receive().map{ //ev ->
                val json = ev.payloadAsText
                val graphQLRequest:GraphQLRequest= objectMapper.readValue(json, GraphQLRequest::class.java)
                val result = handler.execute_react(graphQLRequest.query, graphQLRequest.params, graphQLRequest.operationName, ctx=null)
                }
                */
                //mapOf("data" to result.getData<Any>())


                // repo fun: Flux<Entity>
                // handler.execute_react: ExecutionResult
                // RESTFUL Controller: mapOf("data" to result.getData<Any>())
            }


        }

        return WebSocketHandler {  session ->

            val publisher = Flux.create(Consumer<FluxSink<WebSocketMessage>> { sink ->
                connections[session.id] = ForwardingMessageHandler(session, sink)
                incomingChannel().subscribe(connections[session.id])

            }).doFinally {
                incomingChannel().unsubscribe(connections[session.id])
                connections.remove(session.id)
            }
            session.send(publisher)
        }

    }

}

data class FileEvent(val sessionId: String, val path:String)