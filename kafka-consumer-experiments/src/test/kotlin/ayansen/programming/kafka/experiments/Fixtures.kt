/**
 * Copyright 2020.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ayansen.programming.kafka.experiments

import ayansen.programming.avro.SampleEvent
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

data class KeyValueTimestamp<T, U>(val key: T, val value: U, val timestamp: Long)


object Fixtures {

    const val KAFKA_BROKERS = "localhost:19092"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8083"
    const val APP_GROUP_ID = "test_application"

    fun generateSampleEvents(numberOfEvents: Int, eventName: String): List<KeyValueTimestamp<String, SampleEvent>> =
        (1..numberOfEvents).map {
            val event = SampleEvent.newBuilder()
                .setId(it.toString())
                .setEventName(eventName)
                .build()
            KeyValueTimestamp(eventName, event, it * 100L)
        }


    fun getConsumerProperties(): Properties {
        val config = Properties()
        config[ConsumerConfig.GROUP_ID_CONFIG] = APP_GROUP_ID
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "true"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        return config
    }

    fun getProducerProperties(): Properties {
        val config = Properties()
        config["bootstrap.servers"] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config["acks"] = "all"
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        return config
    }
}