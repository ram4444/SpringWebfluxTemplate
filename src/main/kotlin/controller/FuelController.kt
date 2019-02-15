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
import pl.wendigo.chrome.ChromeProtocol
import pl.wendigo.chrome.InspectablePage
import pl.wendigo.chrome.Inspector
import pl.wendigo.chrome.domain.page.NavigateRequest
import pl.wendigo.chrome.HeadlessSession


import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers
import javax.annotation.PostConstruct

@RestController
class FuelController() {

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
                val fuelcurlHeader_listpairstrany:Map<String,Any>? = fuelcurlHeader_map
                val fuelcurlBody_listpairstrany:List<Pair<String,Any?>>? = fuelcurlBody_map.toList()
                val (request, response, result) = url.httpPost(fuelcurlBody_listpairstrany).header(fuelcurlHeader_listpairstrany).responseString()
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
