package main.kotlin.kafka

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import java.lang.Math.abs
import main.kotlin.pojo.MongoSchema.Node

@Service
class ListenerFuncGrouping {
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
    var currentValueOpen = 0F
    var currentValueClose = 0F
    var currentValueHigh = 0F
    var currentValueLow = 0F
    var currentValueVolume = 0F

    // Here is some cal value to be put to the JSON
    var outraDelta:Float?=null
    var predictIntraDayDelta:Float?=null
    var surplusPredictHighMidPt:Float?=null
    var surplusPredictLowMidPt:Float?=null

    companion object currentVar{
        var valueStackOpen:ValueStack = ValueStack("OPEN")
        var valueStackClose:ValueStack = ValueStack("CLOSE")
        var valueStackHigh:ValueStack = ValueStack("HIGH")
        var valueStackLow:ValueStack = ValueStack("LOW")
        var valueStackVolume:ValueStack = ValueStack("VOLUME")
    }

    data class ValueStack(val name:String) {
        var lastSrcValue:Float?=null
        var lastDeltaValue:Float?=null
        var lastRocDeltaValue:Float?=null //TODO:Get the lastRocDeltaValue

        var lastGRWA:Float?=null
        var lastGRWASort:Float?=null
        var lastGRWAMin:Float?=null
        var lastGRWAMinSort:Float?=null

        var lastPredict7when6:Float?=null
        var lastPredict7when6withDeltaGRWA:Float?=null
        var lastPredict7when6MidPt:Float?=null

        var lastPredict7when6Sort:Float?=null
        var lastPredict7when6withDeltaGRWASort:Float?=null
        var lastPredict7when6SortMidPt:Float?=null

        //var lastPredictOutra:Float?=null

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

        var rowValue: RowValue = RowValue(name)
        //var rowValueClose: RowValue = RowValue("rowValueClose")
        //var rowValueHigh: RowValue = RowValue("rowValueHigh")
        //var rowValueLow: RowValue = RowValue("rowValueLow")
        //var rowValueVolume: RowValue = RowValue("rowValueVolume")
    }

    data class RowValue(val name:String) {

        //TODO:Date, pinDate

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
        var disperSrcPredictGRWAMidPtPcnt: Float? = null
        var disperSrcPredictGRWASortRemainPcnt: Float? = null
        var disperSrcPredictGRWASortwithDeltaPcnt: Float? = null
        var disperSrcPredictGRWASortMidPtPcnt: Float? = null

        var disperSrcPredictGRWAMinRemainPcnt: Float? = null
        var disperSrcPredictGRWAMinwithDeltaPcnt: Float? = null
        var disperSrcPredictGRWAMinMidPtPcnt: Float? = null
        var disperSrcPredictGRWAMinSortRemainPcnt: Float? = null
        var disperSrcPredictGRWAMinSortwithDeltaPcnt: Float? = null
        var disperSrcPredictGRWAMinSortMidPtPcnt: Float? = null

        //indicator
        var rocD2x: Float? = null

    }

    private val logger = KotlinLogging.logger {}

    fun loopAnalyseAndStore(nodeloopOpen:Node,nodeloopClose:Node,nodeloopHigh:Node,nodeloopLow:Node,nodeloopVolume:Node) {

        currentValueOpen=nodeloopOpen.value.toFloat()
        currentValueClose=nodeloopClose.value.toFloat()
        currentValueHigh=nodeloopHigh.value.toFloat()
        currentValueLow=nodeloopLow.value.toFloat()
        currentValueVolume=nodeloopVolume.value.toFloat()

        //-------------debug are for Last insert value---------------
        logger.debug { "-------------Prediction before OPEN for date: ${nodeloopOpen.pinTS}-------------" }
        debugAreaForLastInsert(valueStackOpen, "OPEN")
        debugAreaForLastInsert(valueStackClose, "CLOSE")
        debugAreaForLastInsert(valueStackHigh, "HIGH")
        debugAreaForLastInsert(valueStackLow, "LOW")
        debugAreaForLastInsert(valueStackVolume, "VOLUME")

        //----------------preAnalyseProcess returns Triple(deltaValue, deltaValuePcnt, rocDeltaValue)----------------------
        var triOpen:Triple<Float?,Float?,Float?> = preAnalyseProcess(currentValueOpen, valueStackOpen.lastSrcValue, valueStackOpen.lastDeltaValue)
        var triClose:Triple<Float?,Float?,Float?> = preAnalyseProcess(currentValueClose, valueStackClose.lastSrcValue, valueStackClose.lastDeltaValue)
        var triHigh:Triple<Float?,Float?,Float?> = preAnalyseProcess(currentValueHigh, valueStackHigh.lastSrcValue, valueStackHigh.lastDeltaValue)
        var triLow:Triple<Float?,Float?,Float?> = preAnalyseProcess(currentValueLow, valueStackLow.lastSrcValue, valueStackLow.lastDeltaValue)
        var triVolume:Triple<Float?,Float?,Float?> = preAnalyseProcess(currentValueVolume, valueStackVolume.lastSrcValue, valueStackVolume.lastDeltaValue)

        if (valueStackOpen.lastSrcValue != null) {
            valueStackOpen.rowValue.deltaValue = triOpen.first
            valueStackOpen.rowValue.deltaValuePcnt = triOpen.second
            valueStackOpen.rowValue.rocDeltaValue = triOpen.third
            valueStackOpen.indexMap4GRWADeltaSort.put(abs(triOpen.second!!), currentValueOpen)
            valueStackOpen.lastDeltaValue = triOpen.first
        }

        if (valueStackClose.lastSrcValue != null) {
            valueStackClose.rowValue.deltaValue = triClose.first
            valueStackClose.rowValue.deltaValuePcnt = triClose.second
            valueStackClose.rowValue.rocDeltaValue = triClose.third
            valueStackClose.indexMap4GRWADeltaSort.put(abs(triClose.second!!), currentValueClose)
            valueStackClose.lastDeltaValue = triClose.first
        }

        if (valueStackHigh.lastSrcValue != null) {
            valueStackHigh.rowValue.deltaValue = triHigh.first
            valueStackHigh.rowValue.deltaValuePcnt = triHigh.second
            valueStackHigh.rowValue.rocDeltaValue = triHigh.third
            valueStackHigh.indexMap4GRWADeltaSort.put(abs(triHigh.second!!), currentValueHigh)
            valueStackHigh.lastDeltaValue = triHigh.first
        }

        if (valueStackLow.lastSrcValue != null) {
            valueStackLow.rowValue.deltaValue = triLow.first
            valueStackLow.rowValue.deltaValuePcnt = triLow.second
            valueStackLow.rowValue.rocDeltaValue = triLow.third
            valueStackLow.indexMap4GRWADeltaSort.put(abs(triLow.second!!), currentValueLow)
            valueStackLow.lastDeltaValue = triLow.first
        }

        if (valueStackVolume.lastSrcValue != null) {
            valueStackVolume.rowValue.deltaValue = triVolume.first
            valueStackVolume.rowValue.deltaValuePcnt = triVolume.second
            valueStackVolume.rowValue.rocDeltaValue = triVolume.third
            valueStackVolume.indexMap4GRWADeltaSort.put(abs(triVolume.second!!), currentValueVolume)
            valueStackVolume.lastDeltaValue = triVolume.first
        }


        //------------------------INDICATION AREA-------------------------------------
        // Consume the last predict, The following values reflex optimistic when POSITIVE

        // rocDelta D2x
        if (valueStackOpen.lastRocDeltaValue != null) {
            valueStackOpen.rowValue.rocD2x = valueStackOpen.rowValue.rocDeltaValue!! - valueStackOpen.lastRocDeltaValue!!
        }

        // Overnight Delta
        if (valueStackClose.lastSrcValue != null){
            outraDelta = currentValueOpen - valueStackClose.lastSrcValue!!
        }

        // This predict the trend of intra day
        if (valueStackClose.lastPredict7when6MidPt !=null) {
            predictIntraDayDelta = valueStackClose.lastPredict7when6MidPt!! - currentValueOpen
        }

        // When OPEN has been already higher than tje Predict High, Is it optimistic or will drop back?
        if (valueStackHigh.lastPredict7when6MidPt != null) {
            surplusPredictHighMidPt = currentValueOpen - valueStackHigh.lastPredict7when6MidPt!!
        }

        // When Open has already been lower than the Predict Low, This show persimistic when NEGATIVE
        if (valueStackLow.lastPredict7when6MidPt !=null) {
            surplusPredictLowMidPt = currentValueOpen - valueStackLow.lastPredict7when6MidPt!!
        }
        /* At the point we gather
                valueStackOpen.rowValueOpen.rocD2x
                outraDelta
                predictIntraDayDelta
                surplusPredictHighMidPt
                surplusPredictLowMidPt
         */
        //------------------------INDICATION AREA END----------------------------------

        valueStackOpen.lastSrcValue = currentValueOpen
        valueStackClose.lastSrcValue = currentValueClose
        valueStackHigh.lastSrcValue = currentValueHigh
        valueStackLow.lastSrcValue = currentValueLow
        valueStackVolume.lastSrcValue = currentValueVolume

        //When it comes to OPEN
        valueStackOpen = processRow(currentValueOpen, valueStackOpen)
        logger.debug { "-------------Prediction AFTER OPEN for date: ${nodeloopOpen.pinTS}-------------" }
        debugArea(valueStackOpen.rowValue)

        //After calculation we get a new value for
        /*
        rowValueOpen.rocDeltaValue
        rowValueOpen.deltaValuePcnt

        lastPredict7when6Open
        lastpredict7when6withDeltaGRWAOpen
        lastPredict7when6MidPtOpen

        lastPredict7when6SortOpen
        lastpredict7when6withDeltaGRWASortOpen
        lastPredict7when6SortMidPtOpen
         */

        //So it shows optimistic when the following value is POSITIVE
        /*
        lastPredict7when6Open-lastPredict7when6Close
        lastPredict7when6Open-lastPredict7when6High

        So it shows persimistic when the following value is Negative
        lastPredict7when6Open-lastPredict7when6Low
         */
        // All lastValue in valueStack is updated to latest at the pt.


        //TODO: When A real time value comes in

        //When it comes to Close
        valueStackClose = processRow(currentValueClose, valueStackClose)
        valueStackHigh = processRow(currentValueHigh, valueStackHigh)
        valueStackLow = processRow(currentValueLow, valueStackLow)
        valueStackVolume = processRow(currentValueVolume, valueStackVolume)

        logger.debug { "-------------Prediction AFTER CLOSE for date: ${nodeloopClose.pinTS}-------------" }
        debugArea(valueStackClose.rowValue)
        debugArea(valueStackHigh.rowValue)
        debugArea(valueStackLow.rowValue)
        debugArea(valueStackVolume.rowValue)
    }

    fun preAnalyseProcess(currentValue:Float, lastSrcValue:Float?, lastDeltaValue:Float?):Triple<Float?,Float?,Float?>{
        if (lastSrcValue != null) {
            var deltaValue = currentValue - lastSrcValue!!
            var deltaValuePcnt = deltaValue!! / lastSrcValue!!
            var rocDeltaValue:Float? = null
            if (lastDeltaValue != null) {
                rocDeltaValue = deltaValue!! - lastDeltaValue!!
            }

            return Triple(deltaValue, deltaValuePcnt, rocDeltaValue)

        }
        return Triple(null,null,null)
    }

    fun processRow(currentValue:Float, valueStack:ValueStack):ValueStack {
        var valueStackTemp:ValueStack = valueStack
        // This simulate a OPEN value comes in
        // So that a predict of OPEN valueS can be generated for the next interval
        //Target functionalize ----------------------------------

        //TODO: Adding the disperency to the predict

        valueStackTemp.rowValue.srcValue = currentValue

        valueStackTemp.loopListSrcValue4GRWA.add(currentValue)
        valueStackTemp.loopListSrcValue4GRWAMin.add(currentValue)
        valueStackTemp.loopListSrcValue4GRWASort.add(currentValue)
        valueStackTemp.loopListSrcValue4GRWAMinSort.add(currentValue)

        valueStackTemp.loopListSrcValue4DeltaGRWA.add(currentValue)
        valueStackTemp.loopListSrcValue4DeltaGRWAMin.add(currentValue)
        valueStackTemp.loopListSrcValue4DeltaGRWASort.add(currentValue)
        valueStackTemp.loopListSrcValue4DeltaGRWAMinSort.add(currentValue)

        valueStackTemp.lastSrcValue = currentValue

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

        if (valueStackTemp.indexMap4GRWADeltaSort.size>4) {

            valueStackTemp.indexMap4GRWADeltaSort.iterator().forEach {
                if (loopCnt==0) {
                    earliestKey=it.key
                    loopCnt++
                }
            }
            loopCnt==0

            valueStackTemp.sortIndexMap4GRWADeltaSort = valueStackTemp.indexMap4GRWADeltaSort.toSortedMap(compareByDescending { it })
            valueStackTemp.sortIndexMap4GRWADeltaSort.iterator().forEach {
                loopCnt++
                if (loopCnt==valueStackTemp.sortIndexMap4GRWADeltaSort.size) {
                    smallestKey=it.key
                    //This key is for remove the smallest Element of the MAP
                }
            }
        }

        if (valueStackTemp.loopListSrcValue4GRWA.size>4) {

            //Calculate GRWA
            valueStackTemp.rowValue.GRWA=0F
            loopIndex=0
            for (loopSrcValue in valueStackTemp.loopListSrcValue4GRWA) {
                //logger.debug { "------Calculation of GRWA---------------" }
                //logger.debug { "loopSrcValue: ${loopSrcValue}" }
                //logger.debug { "rowValueOpen.GRWA: ${rowValueOpen.GRWA}" }
                //logger.debug { "rstGoldRatioList[loopIndex]: ${rstGoldRatioList[loopIndex]}" }
                valueStackTemp.rowValue.GRWA = valueStackTemp.rowValue.GRWA!!+(loopSrcValue*rstGoldRatioList[loopIndex])
                loopIndex++
            }
            //logger.debug { "------Calculation of GRWA END----------Result: ${rowValueOpen.GRWA}-----" }
            loopIndex=0

            //Calculate GRWASort
            valueStackTemp.loopListSrcValue4GRWASort = valueStackTemp.loopListSrcValue4GRWA.toMutableList()
            valueStackTemp.rowValue.GRWASort=0F
            //logger.debug { "------------sortIndexMap4GRWADeltaSort:-------------" }
            valueStackTemp.sortIndexMap4GRWADeltaSort.iterator().forEach {
                //REMARKS: The first time calculation of GRWASort is not correct since we only have 4 value for the Delta
                //logger.debug { "${rowValueOpen.GRWASort} \t  ${it.key} \t ${it.value} \t ${rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]}"}
                valueStackTemp.rowValue.GRWASort = valueStackTemp.rowValue.GRWASort!!+it.value*rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]
                loopIndex++
            }
            loopIndex=0

            //When Reading the 6th value
            if (valueStackTemp.loopListSrcValue4DeltaGRWA.size>5) {

                //Calculate the deltaGRWA
                if (valueStackTemp.lastGRWA != null) {
                    valueStackTemp.rowValue.deltaGRWA = valueStackTemp.rowValue.GRWA!! - valueStackTemp.lastGRWA!!
                    if (valueStackTemp.rowValue.deltaGRWA!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWA Remain same
                    //loopListSrcValue4GRWA[0,1] will be dropped
                    val predict7when6=(valueStackTemp.rowValue.GRWA!!-(
                            valueStackTemp.loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1

                    //For GRWA keep its down or up trend
                    val predict7when6withDeltaGRWA=(valueStackTemp.rowValue.GRWA!!+valueStackTemp.rowValue.deltaGRWA!!-(
                            valueStackTemp.loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3
                                    +valueStackTemp.loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1


                    //Calculate the disperency between actual and prediction of last time
                    if (valueStackTemp.lastPredict7when6 != null){
                        valueStackTemp.rowValue.disperSrcPredictGRWARemainPcnt=(valueStackTemp.lastPredict7when6!!-currentValue)/currentValue
                    }
                    if (valueStackTemp.lastPredict7when6withDeltaGRWA != null){
                        valueStackTemp.rowValue.disperSrcPredictGRWAwithDeltaPcnt=(valueStackTemp.lastPredict7when6withDeltaGRWA!!-currentValue)/currentValue
                    }
                    if (valueStackTemp.lastPredict7when6MidPt != null) {
                        valueStackTemp.rowValue.disperSrcPredictGRWAMidPtPcnt = (valueStackTemp.lastPredict7when6MidPt!! - currentValue) / currentValue
                    }

                    //update the last predict
                    valueStackTemp.lastPredict7when6 = predict7when6
                    valueStackTemp.lastPredict7when6withDeltaGRWA = predict7when6withDeltaGRWA
                    valueStackTemp.lastPredict7when6MidPt = (predict7when6 + predict7when6withDeltaGRWA)/2

                }

                //--------------------------------------------------------------------------------------------

                //Calculate the deltaGRWAsort
                if (valueStackTemp.lastGRWASort != null) {
                    valueStackTemp.rowValue.deltaGRWASort = valueStackTemp.rowValue.GRWASort!! - valueStackTemp.lastGRWASort!!
                    if (valueStackTemp.rowValue.deltaGRWASort!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWASort Remain same
                    //loopListSrcValue4GRWASort[0,1] will be dropped
                    val predict7when6Sort=(valueStackTemp.rowValue.GRWASort!!-(
                            valueStackTemp.loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1

                    //For GRWASort keep its down or up trend
                    val predict7when6withDeltaGRWASort=(valueStackTemp.rowValue.GRWASort!!+valueStackTemp.rowValue.deltaGRWASort!!-(
                            valueStackTemp.loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3
                                    +valueStackTemp.loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1

                    //Calculate the disperency between actual and prediction of last time
                    if (valueStackTemp.lastPredict7when6Sort != null){
                        valueStackTemp.rowValue.disperSrcPredictGRWASortRemainPcnt=(valueStackTemp.lastPredict7when6Sort!!-currentValue)/currentValue
                    }
                    if (valueStackTemp.lastPredict7when6withDeltaGRWASort != null){
                        valueStackTemp.rowValue.disperSrcPredictGRWASortwithDeltaPcnt=(valueStackTemp.lastPredict7when6withDeltaGRWASort!!-currentValue)/currentValue
                    }
                    if (valueStackTemp.lastPredict7when6SortMidPt != null) {
                        valueStackTemp.rowValue.disperSrcPredictGRWASortMidPtPcnt = (valueStackTemp.lastPredict7when6SortMidPt!! - currentValue) / currentValue
                    }

                    //update the last predict
                    valueStackTemp.lastPredict7when6Sort = predict7when6Sort
                    valueStackTemp.lastPredict7when6withDeltaGRWASort = predict7when6withDeltaGRWASort
                    valueStackTemp.lastPredict7when6SortMidPt = (predict7when6Sort + predict7when6withDeltaGRWASort)/2
                }

                //Still necessary?
                valueStackTemp.lastGRWASort=valueStackTemp.rowValue.GRWASort

                valueStackTemp.loopListSrcValue4DeltaGRWA.removeAt(0)
                valueStackTemp.loopListSrcValue4DeltaGRWASort.removeAt(0)

            }
            valueStackTemp.lastGRWA=valueStackTemp.rowValue.GRWA
            valueStackTemp.lastGRWASort=valueStackTemp.rowValue.GRWASort
            //------------------------------------------------------------------

            //POP out the First-most inserted element
            valueStackTemp.loopListSrcValue4GRWA.removeAt(0)
            valueStackTemp.loopListSrcValue4GRWASort.removeAt(0)

            valueStackTemp.indexMap4GRWADeltaSort.remove(earliestKey)

        }
        //TODO: For GRWAMin
        /*
        if (loopListSrcValue4GRWAMin.size>7) {

        }
        */

        if (valueStackTemp.rowValue.rocDeltaValue != null){
            valueStackTemp.lastRocDeltaValue = valueStackTemp.rowValue.rocDeltaValue
        }

        //After market predict
        //lastPredictDeltaOutra = valueStackTemp.lastPredict7when6SortMidPt!! - valueStackClose.rowValueClose.srcValue!!

        //println("-----consumer end process end for Topic src-node-open----------")
        return valueStackTemp
    }

    /*
    fun processOpen(currentValue:Float) {
        // This simulate a OPEN value comes in
        // So that a predict of OPEN valueS can be generated for the next interval
        //Target functionalize ----------------------------------

        //TODO: Adding the disperency to the predict

        valueStackOpen.rowValue.srcValue = currentValue

        valueStackOpen.loopListSrcValue4GRWA.add(currentValue)
        valueStackOpen.loopListSrcValue4GRWAMin.add(currentValue)
        valueStackOpen.loopListSrcValue4GRWASort.add(currentValue)
        valueStackOpen.loopListSrcValue4GRWAMinSort.add(currentValue)

        valueStackOpen.loopListSrcValue4DeltaGRWA.add(currentValue)
        valueStackOpen.loopListSrcValue4DeltaGRWAMin.add(currentValue)
        valueStackOpen.loopListSrcValue4DeltaGRWASort.add(currentValue)
        valueStackOpen.loopListSrcValue4DeltaGRWAMinSort.add(currentValue)

        valueStackOpen.lastSrcValue = currentValue

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

        if (valueStackOpen.indexMap4GRWADeltaSort.size>4) {

            valueStackOpen.indexMap4GRWADeltaSort.iterator().forEach {
                if (loopCnt==0) {
                    earliestKey=it.key
                    loopCnt++
                }
            }
            loopCnt==0

            valueStackOpen.sortIndexMap4GRWADeltaSort = valueStackOpen.indexMap4GRWADeltaSort.toSortedMap(compareByDescending { it })
            valueStackOpen.sortIndexMap4GRWADeltaSort.iterator().forEach {
                loopCnt++
                if (loopCnt==valueStackOpen.sortIndexMap4GRWADeltaSort.size) {
                    smallestKey=it.key
                    //This key is for remove the smallest Element of the MAP
                }
            }
        }

        if (valueStackOpen.loopListSrcValue4GRWA.size>4) {

            //Calculate GRWA
            valueStackOpen.rowValue.GRWA=0F
            loopIndex=0
            for (loopSrcValue in valueStackOpen.loopListSrcValue4GRWA) {
                //logger.debug { "------Calculation of GRWA---------------" }
                //logger.debug { "loopSrcValue: ${loopSrcValue}" }
                //logger.debug { "rowValueOpen.GRWA: ${rowValueOpen.GRWA}" }
                //logger.debug { "rstGoldRatioList[loopIndex]: ${rstGoldRatioList[loopIndex]}" }
                valueStackOpen.rowValue.GRWA = valueStackOpen.rowValue.GRWA!!+(loopSrcValue*rstGoldRatioList[loopIndex])
                loopIndex++
            }
            //logger.debug { "------Calculation of GRWA END----------Result: ${rowValueOpen.GRWA}-----" }
            loopIndex=0

            //Calculate GRWASort
            valueStackOpen.loopListSrcValue4GRWASort = valueStackOpen.loopListSrcValue4GRWA.toMutableList()
            valueStackOpen.rowValue.GRWASort=0F
            //logger.debug { "------------sortIndexMap4GRWADeltaSort:-------------" }
            valueStackOpen.sortIndexMap4GRWADeltaSort.iterator().forEach {
                //REMARKS: The first time calculation of GRWASort is not correct since we only have 4 value for the Delta
                //logger.debug { "${rowValueOpen.GRWASort} \t  ${it.key} \t ${it.value} \t ${rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]}"}
                valueStackOpen.rowValue.GRWASort = valueStackOpen.rowValue.GRWASort!!+it.value*rstGoldRatioList[rstGoldRatioList.size-1-loopIndex]
                loopIndex++
            }
            loopIndex=0

            //When Reading the 6th value
            if (valueStackOpen.loopListSrcValue4DeltaGRWA.size>5) {

                //Calculate the deltaGRWA
                if (valueStackOpen.lastGRWA != null) {
                    valueStackOpen.rowValue.deltaGRWA = valueStackOpen.rowValue.GRWA!! - valueStackOpen.lastGRWA!!
                    if (valueStackOpen.rowValue.deltaGRWA!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWA Remain same
                    //loopListSrcValue4GRWA[0,1] will be dropped
                    val predict7when6=(valueStackOpen.rowValue.GRWA!!-(
                            valueStackOpen.loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1

                    //For GRWA keep its down or up trend
                    val predict7when6withDeltaGRWA=(valueStackOpen.rowValue.GRWA!!+valueStackOpen.rowValue.deltaGRWA!!-(
                            valueStackOpen.loopListSrcValue4DeltaGRWA[2]*rstGoldRatio5
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[3]*rstGoldRatio4
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[4]*rstGoldRatio3
                                    +valueStackOpen.loopListSrcValue4DeltaGRWA[5]*rstGoldRatio2))/rstGoldRatio1


                    //Calculate the disperency between actual and prediction of last time
                    if (valueStackOpen.lastPredict7when6 != null){
                        valueStackOpen.rowValue.disperSrcPredictGRWARemainPcnt=(valueStackOpen.lastPredict7when6!!-currentValue)/currentValue
                    }
                    if (valueStackOpen.lastPredict7when6withDeltaGRWA != null){
                        valueStackOpen.rowValue.disperSrcPredictGRWAwithDeltaPcnt=(valueStackOpen.lastPredict7when6withDeltaGRWA!!-currentValue)/currentValue
                    }
                    if (valueStackOpen.lastPredict7when6MidPt != null) {
                        valueStackOpen.rowValue.disperSrcPredictGRWAMidPtPcnt = (valueStackOpen.lastPredict7when6MidPt!! - currentValue) / currentValue
                    }

                    //update the last predict
                    valueStackOpen.lastPredict7when6 = predict7when6
                    valueStackOpen.lastPredict7when6withDeltaGRWA = predict7when6withDeltaGRWA
                    valueStackOpen.lastPredict7when6MidPt = (predict7when6 + predict7when6withDeltaGRWA)/2

                }

                //--------------------------------------------------------------------------------------------

                //Calculate the deltaGRWAsort
                if (valueStackOpen.lastGRWASort != null) {
                    valueStackOpen.rowValue.deltaGRWASort = valueStackOpen.rowValue.GRWASort!! - valueStackOpen.lastGRWASort!!
                    if (valueStackOpen.rowValue.deltaGRWASort!! >0) {
                        //Raising
                    } else {
                        //Dropping
                    }

                    //For GRWASort Remain same
                    //loopListSrcValue4GRWASort[0,1] will be dropped
                    val predict7when6Sort=(valueStackOpen.rowValue.GRWASort!!-(
                            valueStackOpen.loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1

                    //For GRWASort keep its down or up trend
                    val predict7when6withDeltaGRWASort=(valueStackOpen.rowValue.GRWASort!!+valueStackOpen.rowValue.deltaGRWASort!!-(
                            valueStackOpen.loopListSrcValue4DeltaGRWASort[2]*rstGoldRatio5
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[3]*rstGoldRatio4
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[4]*rstGoldRatio3
                                    +valueStackOpen.loopListSrcValue4DeltaGRWASort[5]*rstGoldRatio2))/rstGoldRatio1

                    //Calculate the disperency between actual and prediction of last time
                    if (valueStackOpen.lastPredict7when6Sort != null){
                        valueStackOpen.rowValue.disperSrcPredictGRWASortRemainPcnt=(valueStackOpen.lastPredict7when6Sort!!-currentValue)/currentValue
                    }
                    if (valueStackOpen.lastPredict7when6withDeltaGRWASort != null){
                        valueStackOpen.rowValue.disperSrcPredictGRWASortwithDeltaPcnt=(valueStackOpen.lastPredict7when6withDeltaGRWASort!!-currentValue)/currentValue
                    }
                    if (valueStackOpen.lastPredict7when6SortMidPt != null) {
                        valueStackOpen.rowValue.disperSrcPredictGRWASortMidPtPcnt = (valueStackOpen.lastPredict7when6SortMidPt!! - currentValue) / currentValue
                    }

                    //update the last predict
                    valueStackOpen.lastPredict7when6Sort = predict7when6Sort
                    valueStackOpen.lastPredict7when6withDeltaGRWASort = predict7when6withDeltaGRWASort
                    valueStackOpen.lastPredict7when6SortMidPt = (predict7when6Sort + predict7when6withDeltaGRWASort)/2
                }

                //Still necessary?
                valueStackOpen.lastGRWASort=valueStackOpen.rowValue.GRWASort

                valueStackOpen.loopListSrcValue4DeltaGRWA.removeAt(0)
                valueStackOpen.loopListSrcValue4DeltaGRWASort.removeAt(0)

            }
            valueStackOpen.lastGRWA=valueStackOpen.rowValue.GRWA
            valueStackOpen.lastGRWASort=valueStackOpen.rowValue.GRWASort
            //------------------------------------------------------------------

            //POP out the First-most inserted element
            valueStackOpen.loopListSrcValue4GRWA.removeAt(0)
            valueStackOpen.loopListSrcValue4GRWASort.removeAt(0)

            valueStackOpen.indexMap4GRWADeltaSort.remove(earliestKey)

        }
        //TODO: For GRWAMin
        /*
        if (loopListSrcValue4GRWAMin.size>7) {

        }
        */

        if (valueStackOpen.rowValue.rocDeltaValue != null){
            valueStackOpen.lastRocDeltaValue = valueStackOpen.rowValue.rocDeltaValue
        }

        //After market predict
        //lastPredictDeltaOutra = valueStackOpen.lastPredict7when6SortMidPt!! - valueStackClose.rowValueClose.srcValue!!

        debugArea(valueStackOpen.rowValue)
        //println("-----consumer end process end for Topic src-node-open----------")
    }
    */
    fun debugAreaForLastInsert(valueStack:ValueStack, name:String) {
        //logger.debug{"--------------Last insert Value For ${name}---------------"}
        //logger.debug{"lastSrcValue: \t\t\t\t ${valueStack.lastSrcValue}"}
        //logger.debug{"lastGRWA: \t\t\t\t ${valueStack.lastGRWA}"}
        //logger.debug{"lastGRWASort: \t\t\t\t ${valueStack.lastGRWASort}"}
        //logger.debug{"lastGRWAMin: \t\t\t\t ${valueStack.lastGRWAMin}"}
        //logger.debug{"lastGRWAMinSort: \t\t\t\t ${valueStack.lastGRWAMinSort}"}
        logger.debug{"${name} lastPredict7when6: \t\t\t\t ${valueStack.lastPredict7when6} \t ${valueStack.lastPredict7when6MidPt} \t ${valueStack.lastPredict7when6withDeltaGRWA}  "}
        logger.debug{"${name} lastPredict7when6Sort: \t\t\t\t ${valueStack.lastPredict7when6Sort} \t ${valueStack.lastPredict7when6SortMidPt} \t ${valueStack.lastPredict7when6withDeltaGRWASort} "}
    }

    fun debugArea(rowValue:RowValue) {
        logger.debug{" -------------Current Value to be insert to ${rowValue.name}--------------"}
        logger.debug{" srcValue: \t\t\t ${rowValue.srcValue}"}
        //logger.debug{" deltaValue: \t\t\t ${rowValue.deltaValue}"}
        //logger.debug{" deltaValuePcnt: \t\t\t ${rowValue.deltaValuePcnt}"}
        //logger.debug{" rocDeltaValue: \t\t\t ${rowValue.rocDeltaValue}"}

        //logger.debug{" GRWA: \t\t\t ${rowValue.GRWA}"}
        //logger.debug{" deltaGRWA: \t\t\t ${rowValue.deltaGRWA}"}
        //logger.debug{" GRWAMin: \t\t\t ${rowValue.GRWAMin}"}
        //logger.debug{" deltaGRWAMin: \t\t\t ${rowValue.deltaGRWAMin}"}
        //logger.debug{" GRWASort: \t\t\t ${rowValue.GRWASort}"}
        //logger.debug{" deltaGRWASort: \t\t\t ${rowValue.deltaGRWASort}"}
        //logger.debug{" GRWAMinSort: \t\t\t ${rowValue.GRWAMinSort}"}
        //logger.debug{" deltaGRWAMinSort: \t\t\t ${rowValue.deltaGRWAMinSort}"}

        logger.debug{" disperSrcPredictGRWARemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWARemainPcnt}"}
        logger.debug{" disperSrcPredictGRWAMidPtPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMidPtPcnt}"}
        logger.debug{" disperSrcPredictGRWAwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAwithDeltaPcnt}"}

        logger.debug{" disperSrcPredictGRWASortRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWASortRemainPcnt}"}
        logger.debug{" disperSrcPredictGRWASortMidPtPcnt: \t\t\t ${rowValue.disperSrcPredictGRWASortMidPtPcnt}"}
        logger.debug{" disperSrcPredictGRWASortwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWASortwithDeltaPcnt}"}

        //logger.debug{" disperSrcPredictGRWAMinRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinRemainPcnt}"}
        //logger.debug{" disperSrcPredictGRWAMinwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinwithDeltaPcnt}"}
        //logger.debug{" disperSrcPredictGRWAMinSortRemainPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinSortRemainPcnt}"}
        //logger.debug{" disperSrcPredictGRWAMinSortwithDeltaPcnt: \t\t\t ${rowValue.disperSrcPredictGRWAMinSortwithDeltaPcnt}"}
        logger.debug{" -------------End of Current Value to be insert--------------"}
    }

    /*
    @KafkaListener(topics = ["src-node-open"], groupId = "src")
    fun listen(message: GenericRecord) {
        currentValueOpen=message.get("value") as Float
        //TODO: When a realtime data get in
    }
     */
}
