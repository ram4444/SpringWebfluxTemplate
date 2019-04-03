package main.kotlin.kafka

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class Listener {
    final val goldenRatio = 1/1.6180339F
    val goldenRatioMin = 1-goldenRatio
    val rstGoldRatio1 = goldenRatio
    val rstGoldRatio2 = (1-rstGoldRatio1)*goldenRatio
    val rstGoldRatio3 = (1-rstGoldRatio1-rstGoldRatio2)*goldenRatio
    val rstGoldRatio4 = (1-rstGoldRatio1-rstGoldRatio2-rstGoldRatio3)*goldenRatio
    val rstGoldRatio5 = (1-rstGoldRatio1-rstGoldRatio2-rstGoldRatio3-rstGoldRatio4)*goldenRatio

    val rstGoldRatioMin1 = goldenRatioMin
    val rstGoldRatioMin2 = (1-rstGoldRatioMin1)*goldenRatioMin
    val rstGoldRatioMin3 = (1-rstGoldRatioMin1-rstGoldRatioMin2)*goldenRatioMin
    val rstGoldRatioMin4 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3)*goldenRatioMin
    val rstGoldRatioMin5 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4)*goldenRatioMin
    val rstGoldRatioMin6 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5)*goldenRatioMin
    val rstGoldRatioMin7 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5-rstGoldRatioMin6)*goldenRatioMin
    val rstGoldRatioMin8 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5-rstGoldRatioMin6-rstGoldRatioMin7)*goldenRatioMin

    var currentValue = 0F
    companion object valueStack {

        var lastSrcValue:Float?=null
        var lastDeltaValue:Float?=null

        var loopListSrcValue4GRWA:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWAMin:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWASort:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWAMinSort:MutableList<Float> = mutableListOf()

        var loopListGRWAComponent:MutableList<Float> = mutableListOf()
        var loopListGRWAMinComponent:MutableList<Float> = mutableListOf()
        var loopListGRWASortComponent:MutableList<Float> = mutableListOf()
        var loopListGRWAMinSortComponent:MutableList<Float> = mutableListOf()
        var predictGRWAEqual:Float? = null
        var predictGRWAwDelta:Float? = null
        var predictGRWAMinEqual:Float? = null
        var predictGRWAMinwDelta:Float? = null
        var predictGRWASortEqual:Float? = null
        var predictGRWASortwDelta:Float? = null
        var predictGRWAMinSortEqual:Float? = null
        var predictGRWAMinSortwDelta:Float? = null
    }

    object RowValue {
        var srcValue: Float? = null
        var deltaValue: Float? = null
        var deltaValuePcnt: Float? = null
        var rocDeltaValue: Float? = null
        var GRWA: Float? = null
        var deltaGRWA: Float? = null
        var GRWAMin: Float? = null
        var deltaGRWAMin: Float? = null
        var GRWASort: Float? = null
        var deltaGRWASort: Float? = null
        var GRWAMinSort: Float? = null
        var deltaGRWAMinSort: Float? = null
    }

    //TODO: para the topic and group
    @KafkaListener(topics = ["src-node-open"], groupId = "src")
    fun listen(message: GenericRecord) {
        //println("-----consumer start----------")
        //println("-----consumer end----------")
        //TODO: Consuming Custom Messages

        currentValue=message.get("value") as Float
        var rowValue = RowValue
        rowValue.srcValue = currentValue

        loopListSrcValue4GRWA.add(currentValue)
        loopListSrcValue4GRWAMin.add(currentValue)
        loopListSrcValue4GRWASort.add(currentValue)
        loopListSrcValue4GRWAMinSort.add(currentValue)

        if (lastSrcValue != null) {
            rowValue.deltaValue = currentValue - lastSrcValue!!
            rowValue.deltaValuePcnt = rowValue.deltaValue!! / lastSrcValue!!
            if (lastDeltaValue != null) {
                rowValue.rocDeltaValue = rowValue.deltaValue!! - lastDeltaValue!!
            }
            lastSrcValue = currentValue
            lastDeltaValue = rowValue.deltaValue
        }

        if (loopListSrcValue4GRWA.size>4) {

            //Calculate GRWA
            for (loopSrcValue in loopListSrcValue4GRWA) {
                //TODO: put the above value into a List
                //loopSrcValue*goldratioCurrent
            }

            //Copy & Sort
            loopListSrcValue4GRWASort = loopListSrcValue4GRWA
            loopListSrcValue4GRWASort.sortDescending()
            //Calculate GRWASort

            //arrValue.removeAt(0)
        }

        if (loopListSrcValue4GRWAMin.size>7) {

        }

    }
}
