package main.kotlin.controller

import org.springframework.web.bind.annotation.*
import pl.wendigo.chrome.ChromeProtocol
import pl.wendigo.chrome.InspectablePage
import pl.wendigo.chrome.Inspector
import pl.wendigo.chrome.domain.page.NavigateRequest
import pl.wendigo.chrome.HeadlessSession

@RestController
class HeadlessChromeController() {

    @PostMapping("/headlesschrome")
    fun loadpage(@RequestHeader url:String){
        val inspector = Inspector.connect("127.0.0.1:9222")
        val protocol = ChromeProtocol.openHeadlessSession(inspector.openedPages().firstOrError().blockingGet())

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
    }

}
