package main.kotlin

import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext

@SpringBootApplication
class SpringWebfluxTemplate
fun main(args: Array<String>) {
    //val applicationContext: ApplicationContext =  SpringApplication.run(SpringTemplate::class.java, *args)
    val app = SpringApplication(SpringWebfluxTemplate::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
