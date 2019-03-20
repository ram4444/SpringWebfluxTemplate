package main.kotlin.kafka

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class Listener {
    @KafkaListener(topics = ["kafkatopic2"], groupId = "foo")
    fun listen(message: String) {
        println("Received Messasge in group foo: $message")
        //TODO: Consuming Custom Messages
    }
}
