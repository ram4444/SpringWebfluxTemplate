package main.kotlin.controller

import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.SingleSource
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.*
import pl.wendigo.chrome.ChromeProtocol
import pl.wendigo.chrome.InspectablePage
import pl.wendigo.chrome.Inspector
import pl.wendigo.chrome.domain.page.NavigateRequest
import pl.wendigo.chrome.HeadlessSession
import pl.wendigo.chrome.domain.dom.GetDocumentRequest
import pl.wendigo.chrome.domain.dom.GetDocumentResponse
import pl.wendigo.chrome.domain.dom.GetFlattenedDocumentRequest
import pl.wendigo.chrome.domain.dom.Node

//@RestController
class HeadlessChromeController() {

    val inspector = Inspector.connect("127.0.0.1:9222")
    val protocol = ChromeProtocol.openHeadlessSession(inspector.openedPages().firstOrError().blockingGet())
    @PostMapping("/headlesschrome")
    fun loadpage(@RequestHeader url:String){


        //val headless = protocol.headless("about:blank", 1280, 1024).blockingGet()

        //println("browserContext: ${headless.browserContextId}")
        //println("target: ${headless.targetId}")

        protocol.Page.enable().blockingGet()


        val event = protocol.Page.navigate(NavigateRequest(url=url)).flatMap{ (frameId) ->
            protocol.Page.frameStoppedLoading().filter {
                it.frameId == frameId
            }
                    .take(1)
                    .singleOrError()
        }.blockingGet()

        println("page loaded: $event")

        //val req:GetFlattenedDocumentRequest = GetFlattenedDocumentRequest(1,null)
        //val node:Node = protocol.DOM.getFlattenedDocument(req)
        val rtn = protocol.DOM.getDocument(GetDocumentRequest(-1,true)).flatMap { (node) ->
            println("here")
            println(node.nodeName)
            println(node.nodeId)
            SingleSource<String> { node.nodeName  }
        }

        println(rtn)
    }

}
