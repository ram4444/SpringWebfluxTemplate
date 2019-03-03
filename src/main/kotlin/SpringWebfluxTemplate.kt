package main.kotlin

import ch.qos.logback.classic.Level
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.scheduling.annotation.EnableScheduling


@SpringBootApplication
@EnableScheduling
class SpringWebfluxTemplate
fun main(args: Array<String>) {
    //val applicationContext: ApplicationContext =  SpringApplication.run(SpringTemplate::class.java, *args)
    val root = org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
    root.level = Level.DEBUG
    val app = SpringApplication(SpringWebfluxTemplate::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
