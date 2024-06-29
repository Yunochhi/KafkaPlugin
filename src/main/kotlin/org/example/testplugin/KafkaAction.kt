package org.example.testplugin

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.intellij.openapi.actionSystem.AnAction
import com.intellij.openapi.actionSystem.AnActionEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.stereotype.Service
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import java.io.File

class KafkaAction : AnAction(){
    override fun actionPerformed(e: AnActionEvent) {
        val kafkaTemplate = KafkaTemplate(producerFactory())
        val kafkaProducer = KafkaProducer(kafkaTemplate)

        kafkaProducer.sendJsonData("Z:\\KafkaPractica\\testPlugin\\RequestData.json")

        val project: Project? = e.project
        if (project != null) {
            Messages.showMessageDialog(project, "Data sent!", "Kafka info", Messages.getInformationIcon())
        }
    }

    private fun producerFactory(): ProducerFactory<String, String> {
        val configProps: MutableMap<String, Any> = mutableMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29094",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )
        return DefaultKafkaProducerFactory(configProps)
    }

    data class JsonData(val topic: String, val key: String, val value: String, val headers: JsonNode)

    @Service
    class KafkaProducer(private val kafkaTemplate: KafkaTemplate<String, String>) {
        fun sendJsonData(filePath: String)
        {
            val jsonFile = File(filePath)
            val mapper = jacksonObjectMapper()
            val jsonData: JsonData = mapper.readValue(jsonFile, JsonData::class.java)

            val headers = jsonData.headers
            val recordHeaders: Headers = RecordHeaders()

            headers.fields().forEach {
                    header -> recordHeaders.add(header.key, header.value.asText().toByteArray())
            }

            val produceRecord = ProducerRecord(jsonData.topic, 0, jsonData.key, jsonData.value, recordHeaders)
            kafkaTemplate.send(produceRecord)
        }
    }
}

