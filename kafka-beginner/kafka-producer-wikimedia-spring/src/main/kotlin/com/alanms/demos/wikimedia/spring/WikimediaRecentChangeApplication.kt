package com.alanms.demos.wikimedia.spring

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class WikimediaSpringApplication

fun main(args: Array<String>) {
	runApplication<WikimediaSpringApplication>(*args)
}
