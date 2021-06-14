package no.nav

import io.ktor.application.*
import io.ktor.routing.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.intellij.lang.annotations.Language
import java.util.*

fun Route.ProduceMessage() {
    get("/produce/{message}") {
        val message = this.call.parameters["message"] ?: "no message! :("

        KafkaProducer<String, String>(getKafkaConfig()).use { producer ->
            // Skriv inn koden for å videresende meldingen fra HTTP requesten og skrive denne til kafka kø her!
        }

    }
}

fun getKafkaConfig(): Properties {
    val env = System.getenv().toProperties()
    val props = Properties()
    props[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = env["KAFKA_BROKERS"]
    props[CommonClientConfigs.CLIENT_ID_CONFIG] = env["NAIS_APP_NAME"]
    props[CommonClientConfigs.GROUP_ID_CONFIG] = env["NAIS_APP_NAME"]

    props[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = SecurityProtocol.SSL.name
    props[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = ""
    props[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = "jks"
    props[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = env["KAFKA_TRUSTSTORE_PATH"]
    props[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = env["KAFKA_CREDSTORE_PATH"]
    props[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = "PKCS12"
    props[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = env["KAFKA_KEYSTORE_PATH"]
    props[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = env["KAFKA_CREDSTORE_PATH"]

    props[ProducerConfig.ACKS_CONFIG] = "all"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName

    return props
}

