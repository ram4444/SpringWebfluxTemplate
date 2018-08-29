package main.kotlin.service

import main.kotlin.pojo.TestEntity
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Flux

@Service
class WebfluxService {
    fun fetchTestEntity(name: String): Flux<TestEntity> = fetch("testentity/$name").bodyToFlux(TestEntity::class.java)

    //fun fetchPosts(): Flux<Post> = fetch("/posts").bodyToFlux(Post::class.java)

    fun fetch(path: String): WebClient.ResponseSpec {
        val client = WebClient.create("http://jsonplaceholder.typicode.com/")
        return client.get().uri(path).retrieve()
    }
}