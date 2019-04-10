package main.kotlin.kafka

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import java.lang.Math.abs

@Service
class Listener {
    final val goldenRatio = 1/1.6180339F
    val goldenRatioMin = 1-goldenRatio
    val rstGoldRatio1 = goldenRatio
    val rstGoldRatio2 = (1-rstGoldRatio1)*goldenRatio
    val rstGoldRatio3 = (1-rstGoldRatio1-rstGoldRatio2)*goldenRatio
    val rstGoldRatio4 = (1-rstGoldRatio1-rstGoldRatio2-rstGoldRatio3)*goldenRatio
    val rstGoldRatio5 = (1-rstGoldRatio1-rstGoldRatio2-rstGoldRatio3-rstGoldRatio4)*goldenRatio
    val rstGoldRatioList: List<Float> = listOf(rstGoldRatio5,rstGoldRatio4,rstGoldRatio3,rstGoldRatio2,rstGoldRatio1)

    val rstGoldRatioMin1 = goldenRatioMin
    val rstGoldRatioMin2 = (1-rstGoldRatioMin1)*goldenRatioMin
    val rstGoldRatioMin3 = (1-rstGoldRatioMin1-rstGoldRatioMin2)*goldenRatioMin
    val rstGoldRatioMin4 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3)*goldenRatioMin
    val rstGoldRatioMin5 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4)*goldenRatioMin
    val rstGoldRatioMin6 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5)*goldenRatioMin
    val rstGoldRatioMin7 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5-rstGoldRatioMin6)*goldenRatioMin
    val rstGoldRatioMin8 = (1-rstGoldRatioMin1-rstGoldRatioMin2-rstGoldRatioMin3-rstGoldRatioMin4-rstGoldRatioMin5-rstGoldRatioMin6-rstGoldRatioMin7)*goldenRatioMin
    val rstGoldRatioMinList: List<Float> = listOf(rstGoldRatioMin8,rstGoldRatioMin7,rstGoldRatioMin6,rstGoldRatioMin5,rstGoldRatioMin4,rstGoldRatioMin3,rstGoldRatioMin2,rstGoldRatioMin1)

    var loopIndex:Int=0
    var currentValue = 0F

    companion object valueStack {

        var lastSrcValue:Float?=null
        var lastDeltaValue:Float?=null
        var lastGRWA:Float?=null
        var lastGRWASort:Float?=null
        var lastGRWAMin:Float?=null
        var lastGRWAMinSort:Float?=null

        var lastPredict7when6:Float?=null
        var lastpredict7when6withDeltaGRWA:Float?=null
        var lastPredict7when6MidPt:Float?=null

        var lastPredict7when6Sort:Float?=null
        var lastpredict7when6withDeltaGRWASort:Float?=null
        var lastPredict7when6SortMidPt:Float?=null

        var loopListSrcValue4GRWA:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWAMin:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWASort:MutableList<Float> = mutableListOf()
        var loopListSrcValue4GRWAMinSort:MutableList<Float> = mutableListOf()

        var loopListSrcValue4DeltaGRWA:MutableList<Float> = mutableListOf()
        var loopListSrcValue4DeltaGRWAMin:MutableList<Float> = mutableListOf()
        var loopListSrcValue4DeltaGRWASort:MutableList<Float> = mutableListOf()
        var loopListSrcValue4DeltaGRWAMinSort:MutableList<Float> = mutableListOf()


        var indexMap4GRWADeltaSort:MutableMap<Float,Float> =mutableMapOf()
        var indexMap4GRWADeltaMinSort:MutableMap<Float,Float> =mutableMapOf()
        var sortIndexMap4GRWADeltaSort:MutableMap<Float,Float> =mutableMapOf()
        var sortIndexMap4GRWADeltaMinSort:MutableMap<Float,Float> =mutableMapOf()

        /*
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
         */
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

        var disperSrcPredictGRWARemainPcnt: Float? = null
        var disperSrcPredictGRWAwithDeltaPcnt: Float? = null
        var disperSrcPredictGRWASortRemainPcnt: Float? = null
        var disperSrcPredictGRWASortwithDeltaPcnt: Float? = null

        var disperSrcPredictGRWAMinRemainPcnt: Float? = null
        var disperSrcPredictGRWAMinwithDeltaPcnt: Float? = null
        var disperSrcPredictGRWAMinSortRemainPcnt: Float? = null
        var disperSrcPredictGRWAMinSortwithDeltaPcnt: Float? = null
    }

    private val logger = KotlinLogging.logger {}

    //TODO: para the topic and group
    @KafkaListener(topics = ["src-node-open"], groupId = "src")
    fun listen(message: GenericRecord) {
        //println("-----consumer receive msg from kafka Topic src-node-open----------")

        //-------------debug are for Last insert value---------------
        logger.debug{"--------------Last insert Value---------------"}
        logger.debug{"lastSrcValue: \t\t\t\t ${lastSrcValue}"}
        logger.debug{"lastGRWA: \t\t\t\t ${lastGRWA}"}
        logger.debug{"lastGRWASort: \t\t\t\t ${lastGRWASort}"}
        logger.debug{"lastGRWAMin: \t\t\t\t ${lastGRWAMin}"}
        logger.debug{"lastGRWAMinSort: \t\t\t\t ${lastGRWAMinSort}"}
        logger.debug{"lastPredict7when6: \t\t\t\t ${lastPredict7when6} \t ${lastpredict7when6withDeltaGRWA} \t ${lastPredict7when6MidPt} "}
        logger.debug{"lastPredict7when6Sort: \t\t\t\t ${lastPredict7when6Sort} \t ${lastpredict7when6withDeltaGRWASort} \t ${lastPredict7when6SortMidPt}"}
        logger.debug{"-----------------------------------------------"}

        currentValue=message.get("value") as Float
        var rowValue = RowValue
        rowValue.srcValue = currentValue

        loopListSrcValue4GRWA.add(currentValue)
        loopListSrcValue4GRWAMin.add(currentValue)
        loopListSrcValue4GRWASort.add(currentValue)
        loopListSrcValue4GRWAMinSort.add(currentValue)

        loopListSrcValue4DeltaGRWA.add(currentValue)
        loopListSrcValue4DeltaGRWAMin.add(currentValue)
        loopListSrcValue4DeltaGRWASort.add(currentValue)
        loopListSrcValue4DeltaGRWAMinSort.add(currentValue)

        if (lastSrcValue != null) {
            rowValue.deltaValue = currentValue - lastSrcValue!!
            rowValue.deltaValuePcnt = rowValue.deltaValue!! / lastSrcValue!!
            if (lastDeltaValue != null) {
                rowValue.rocDeltaValue = rowValue.deltaValue!! - lastDeltaValue!!
            }

            lastDeltaValue = rowValue.deltaValue
            //sortIndexMap4GRWADeltaSort.toSortedMap()
            indexMap4GRWADeltaSort.put(abs(rowValue.deltaValuePcnt!!) , currentValue)

        }
        lastSrcValue = currentValue

        /* When Reading the 5th value
            -Sort the map of (abs(deltaValuePcnt) , src)
            -Calculate GRWA
            -Calculate GRWASort
            When Reading the 6th value
                -Calculate the deltaGRWA
                -Calculate the deltaGRWAsort
         */
        //Sort the map of (abs(deltaValuePcnt) , src)
        var smallestKey= 0F
        var earliestKey:Float?=null
        var loopCnt=0

        if (indexMap4GRWADeltaSort.size>4) {

            indexMap4GRWADeltaSort.iterator().forEach {
                if (loopCnt==0) {
                    earliestKey=it.key
                    loopCnt++
                }
            }
            loopCnt==0

            sortIndexMap4GRWADeltaSort = indexMap4GRWADeltaSort.toSortedMap(compareByDescending { it })
            sortIndexMap4GRWADeltaSort.iterator().forEach {
                loopCnt++
                if (loopCnt==sortIndexMap4GRWADeltaSort.size) {
                    smallestKey=it.key
                    //This key is for remove the smallest Element of the MAP
                }
            }
        }

        if (loopListSrcValue4GRWA.size>4) {

            //Calculate GRWA
            rowValue.GRWA=0F
            loopIndex=0
            for (loopSrcValue in loopListSrcValue4GRWA) {
                //logger.debug { "------Calculation of GRWA---------------" }
                //logger.debug { "loopSrcValue: ${loopSrcValue}" }
                //logger.debug { "rowValue.GRWA: ${rowValue.GRWA}" }
                //logger.debug { "rstGoldRatioList[loopIndex]: ${rstGoldRatioList[loopIndex]}" }
                rowValue.GRWA = rowValue.GRWA!!+(loopSrcValue*rstGoldRatioList[loopIndex])
                loopIndex++
            }
            //logger.debug { "------Calculation of GRWA END----------Result: ${rowValue.GRWA}-----" }
            loopIndex=0

            //Calculate GRWASort
            loopListSrcValue4GRWASort = loopListSrcValue4GRWA.toMutableList()
            rowValue.GRWASort=0F
            //logger.debug { "------------sortIndexMap4GRWADeltaSort:-------------" }
            sortIndexMap4GRWADeltaSort.iterator().forEach {
                //REMARKS: The first time calculation of GRWASort is not correct since we only have 4 value for the Delta
                //logger.debug { "${rowValue.GRWASort} \t  ${it.key} \t ${it.value} \t ${rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]}"}
                rowValue.GRWASort = rowValue.GRWASort!!+it.value*rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]
                loopIndex++
            }
            loopIndex=0

            //When Reading the 6th value
            if (loopListSrcValue4DeltaGRWA.size>5) {
                //Calculate the deltaGRWA
                if (lastGRWA != null) {
                    rowValue.deltaGRWA = rowValue.GRWA!! - lastGRWA!!
                    if (rowValue.deltaGRWA!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWA Remain same
                    //loopListSrcValue4GRWA[0,1] will be dropped
                    val predict7when6=(rowValue.GRWA!!-(loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5+loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4+loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3+loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1
                    if (lastPredict7when6 != null){
                        //Calculate the disperency between actual and prediction of last time
                        rowValue.disperSrcPredictGRWARemainPcnt=(lastPredict7when6!!-currentValue)/currentValue

                    }
                    //update the last predict
                    lastPredict7when6 = predict7when6

                    //For GRWA keep its down or up trend
                    val predict7when6withDeltaGRWA=(rowValue.GRWA!!+rowValue.deltaGRWA!!-(loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5+loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4+loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3+loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1
                    if (lastpredict7when6withDeltaGRWA != null){
                        //Calculate the disperency between actual and prediction of last time
                        rowValue.disperSrcPredictGRWAwithDeltaPcnt=(lastpredict7when6withDeltaGRWA!!-currentValue)/currentValue
                    }
                    lastpredict7when6withDeltaGRWA = predict7when6withDeltaGRWA
                    lastPredict7when6MidPt = (predict7when6 + predict7when6withDeltaGRWA)/2
                }


                //--------------------------------------------------------------------------------------------
                //Calculate the deltaGRWAsort

                if (lastGRWASort != null) {
                    rowValue.deltaGRWASort = rowValue.GRWASort!! - lastGRWASort!!
                    if (rowValue.deltaGRWASort!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWASort Remain same
                    //loopListSrcValue4GRWASort[0,1] will be dropped
                    val predict7when6Sort=(rowValue.GRWASort!!-(loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5+loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4+loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3+loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1
                    if (lastPredict7when6Sort != null){
                        //Calculate the disperency between actual and prediction of last time
                        rowValue.disperSrcPredictGRWASortRemainPcnt=(lastPredict7when6Sort!!-currentValue)/currentValue

                    }
                    //update the last predict
                    lastPredict7when6Sort = predict7when6Sort

                    //For GRWASort keep its down or up trend
                    val predict7when6withDeltaGRWASort=(rowValue.GRWASort!!+rowValue.deltaGRWASort!!-(loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5+loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4+loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3+loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1
                    if (lastpredict7when6withDeltaGRWASort != null){
                        //Calculate the disperency between actual and prediction of last time
                        rowValue.disperSrcPredictGRWASortwithDeltaPcnt=(lastpredict7when6withDeltaGRWASort!!-currentValue)/currentValue
                    }
                    lastpredict7when6withDeltaGRWASort = predict7when6withDeltaGRWASort

                    lastPredict7when6SortMidPt = (predict7when6Sort + predict7when6withDeltaGRWASort)/2

                }
                lastGRWASort=rowValue.GRWASort


                loopListSrcValue4DeltaGRWA.removeAt(0)
                loopListSrcValue4DeltaGRWASort.removeAt(0)

            }
            lastGRWA=rowValue.GRWA
            lastGRWASort=rowValue.GRWASort
            //------------------------------------------------------------------

            //POP out the First-most inserted element
            loopListSrcValue4GRWA.removeAt(0)
            loopListSrcValue4GRWASort.removeAt(0)

            indexMap4GRWADeltaSort.remove(earliestKey)

        }
        //TODO: For GRWAMin
        /*
        if (loopListSrcValue4GRWAMin.size>7) {

        }
        */
        debugArea(rowValue)
        //println("-----consumer end process end for Topic src-node-open----------")
    }

    fun debugArea(rowValue:RowValue) {
        logger.debug{" -------------Current Value to be insert--------------"}
        logger.debug{" srcValue: \t\t\t ${rowValue.srcValue}"}
        logger.debug{" deltaValue: \t\t\t ${rowValue.deltaValue}"}
        logger.debug{" deltaValuePcnt: \t\t\t ${rowValue.deltaValuePcnt}"}
        logger.debug{" rocDeltaValue: \t\t\t ${rowValue.rocDeltaValue}"}

        logger.debug{" GRWA: \t\t\t ${rowValue.GRWA}"}
        logger.debug{" deltaGRWA: \t\t\t ${rowValue.deltaGRWA}"}
        logger.debug{" GRWAMin: \t\t\t ${rowValue.GRWAMin}"}
        logger.debug{" deltaGRWAMin: \t\t\t ${rowValue.deltaGRWAMin}"}
        logger.debug{" GRWASort: \t\t\t ${rowValue.GRWASort}"}
        logger.debug{" deltaGRWASort: \t\t\t ${rowValue.deltaGRWASort}"}
        logger.debug{" GRWAMinSort: \t\t\t ${rowValue.GRWAMinSort}"}
        logger.debug{" deltaGRWAMinSort: \t\t\t ${rowValue.deltaGRWAMinSort}"}

        logger.debug{" disperSrcPredictGRWARemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWARemainPcnt}"}
        logger.debug{" disperSrcPredictGRWAwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAwithDeltaPcnt}"}
        logger.debug{" disperSrcPredictGRWASortRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWASortRemainPcnt}"}
        logger.debug{" disperSrcPredictGRWASortwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWASortwithDeltaPcnt}"}

        logger.debug{" disperSrcPredictGRWAMinRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinRemainPcnt}"}
        logger.debug{" disperSrcPredictGRWAMinwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinwithDeltaPcnt}"}
        logger.debug{" disperSrcPredictGRWAMinSortRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinSortRemainPcnt}"}
        logger.debug{" disperSrcPredictGRWAMinSortwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinSortwithDeltaPcnt}"}
        logger.debug{" -------------End of Current Value to be insert--------------"}
    }
}
