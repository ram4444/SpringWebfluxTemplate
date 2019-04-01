package main.kotlin.kafka

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class Listener {
    companion object valueStack {
        var mean5:Float = 0F
        var sum5:Float = 0F
        var arrValue:MutableList<Float> = mutableListOf()
    }

    //TODO: para the topic and group
    @KafkaListener(topics = ["src-node-open"], groupId = "src")
    fun listen(message: GenericRecord) {
        //println("-----consumer start----------")
        //println("Received Messasge in group foo:")
        //println(message.toString())
        //println(message.get("pinDate"))
        //println(message.get("value"))
        //println("-----consumer end----------")
        //TODO: Consuming Custom Messages
        arrValue.add(message.get("value") as Float)
        if (arrValue.size>4) {
            sum5=0F
            for (item in arrValue) {
                sum5=sum5+item
            }
            mean5 = sum5/5
            //TODO: THis is example, ready to put to another producer
            arrValue.removeAt(0)
        }


    }
}
