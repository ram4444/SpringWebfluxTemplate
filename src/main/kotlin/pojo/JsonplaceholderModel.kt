package main.kotlin.pojo

data class Comment(
        val postId: Int,
        val id: Int,
        val name: String,
        val email: String,
        val body: String
)

data class Post(
        val userId: Int,
        val id: Int,
        val title: String,
        val body: String
)

data class Response(
        val postId: Int,
        val userId: Int,
        val title: String,
        val comments: List<LightComment>
)

data class LightComment(
        val email: String,
        val body: String
)

data class Event(
        val sender: Int,
        val value: Int
)