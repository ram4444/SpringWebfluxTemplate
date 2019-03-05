package main.kotlin.controller

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.fuel.core.FuelManager

import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.httpPost
import com.github.kittinunf.result.Result
import mu.KotlinLogging
import org.springframework.web.bind.annotation.*
import java.net.InetSocketAddress
import java.net.Proxy


@RestController
class FuelController() {

    private val logger = KotlinLogging.logger {}

    //TODO: Rewrite curlasyncbyfuel
    @PostMapping("/curlasyncbyfuel")
    fun curlAsyncByfuel(@RequestHeader method:String, @RequestHeader url:String, @RequestBody requestBody:String):Map<String, Any> {

        logger.info { "Curl is called ASYNC by Fuel" }
        logger.debug { "method:" }; logger.debug { method }
        logger.debug { "url:" }; logger.debug { url }
        logger.debug { "body:" } ; logger.debug { requestBody }

        var rtn:String=""

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
        logger.info { "Curl is called by Fuel" }
        logger.debug { "method:" }; logger.debug { method }
        logger.debug { "url:" }; logger.debug { url }
        logger.debug { "body:" } ; logger.debug { body }

        //For Proxy
        /*
        if (proxy_enable.toLowerCase().equals("true")) {
            logger.info{ "Fuel is working under Proxy Setting" }
            FuelManager.instance.proxy= Proxy(Proxy.Type.HTTP, InetSocketAddress(proxy_host, proxy_port.toInt()))
        }
        */

        val bodymap: Map<String, Map<String,Any>> = jacksonObjectMapper().readValue(body.trim())

        if (method.toLowerCase().equals("get")){
            // Get the header(to be sent) from the body
            //TODO: Implement Param
            //TODO: replace call method to Original method
            val fuelcurlHeader_map:Map<String,Any>? = bodymap.get("header")

            if (null != fuelcurlHeader_map) {
                // Header does exist
                val (request, response, result) = url.httpGet().header(fuelcurlHeader_map).responseString()
                logger.info { "GET with Header" }
                logger.debug { "request:" }; logger.debug { request }
                logger.debug { "response:" }; logger.debug { response }
                logger.debug { "result:" } ; logger.debug { result }
                return mapOf("rtn_code" to response.statusCode,
                            "response" to response.responseMessage,
                            "result" to result.get()
                )
            } else {
                // Header does not exist
                val (request, response, result) = url.httpGet().responseString()
                logger.info { "GET without Header" }
                logger.debug { "request:" }; logger.debug { request }
                logger.debug { "response:" }; logger.debug { response }
                logger.debug { "result:" } ; logger.debug { result }
                return mapOf("rtn_code" to "success", "response" to response.data, "result" to result.get())
            }

        } else if (method.toLowerCase().equals("post")) {
            /* Post method*/
            // Get the header and body (to be sent) from the body
            val fuelcurlHeader_map:Map<String,Any>? = bodymap.get("header")
            val fuelcurlBody_map:Map<String,Any>? = bodymap.get("body")

            if (null != fuelcurlBody_map) {
                val fuelcurlBody_listpairstrany:List<Pair<String,Any?>>? = fuelcurlBody_map.toList()
                val (request, response, result) = Fuel.post(url)
                        .header(fuelcurlHeader_map)
                        .body(jacksonObjectMapper().writeValueAsString(fuelcurlBody_map))
                        .responseString()

                logger.info { "POST with Request Body" }
                logger.debug { "request:" }; logger.debug { request }
                logger.debug { "response:" }; logger.debug { response }
                logger.debug { "result:" } ; logger.debug { result }

                return mapOf("rtn_code" to response.statusCode,
                        "response" to response.responseMessage,
                        "result" to result
                )
            } else {
                val (request, response, result) = Fuel.post(url)
                        .header(fuelcurlHeader_map)
                        .responseString()
                logger.info { "POST without Request Body" }
                logger.debug { "request:" }; logger.debug { request }
                logger.debug { "response:" }; logger.debug { response }
                logger.debug { "result:" } ; logger.debug { result }

                return mapOf("rtn_code" to response.statusCode,
                        "response" to response.responseMessage,
                        "result" to result
                )
            }
        } else {
            logger.error { "Other method is not support" }
            return mapOf("rtn_code" to "fail", "msg" to "Other methmod is not allow")
        }
    }

}
