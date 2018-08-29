package main.kotlin.handler

import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import main.kotlin.pojo.Event

@Component
class PrimeNumbersHandler : WebSocketHandler {
    private val processor = TopicProcessor.share<Event>("shared", 1024)
    override fun handle(session: WebSocketSession): Mono<Void> {
        //Client side send msg like this [clientId:randdomnum]
        return session.send( //this is for sending back msg
                processor
                        .map { ev -> session.textMessage("${ev.sender}:${ev.value}") }
        ).and(
                session.receive()
                        .map { ev ->
                            val parts = ev.payloadAsText.split(":")
                            Event(sender = parts[0].toInt(), value = parts[1].toInt())

                        }
                        //.filter { ev -> isPrime(ev.value) }
                        .doOnNext { ev -> processor.onNext(ev) }
        )

    }
}