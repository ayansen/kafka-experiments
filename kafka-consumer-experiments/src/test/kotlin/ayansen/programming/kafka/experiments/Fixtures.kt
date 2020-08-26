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
import ayansen.programming.avro.SampleEvent.newBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.util.*


object Fixtures {

    const val KAFKA_BROKERS = "localhost:19092"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8083"
    const val APP_GROUP_ID = "test_application"
    const val CLIENT_ID = "test_client"

    fun generateSampleEvents(numberOfEvents: Int, eventName: String): List<SampleEvent> =
        (1..numberOfEvents).map {
            newBuilder()
                .setId(it.toString())
                .setEventName(eventName)
                .build()
        }

    fun <K, V> getTestProducer(keySerializer: Serializer<K>, valueSerializer: Serializer<V>): KafkaProducer<K, V> {
        val config = Properties()
        config["client.id"] = CLIENT_ID
        config["bootstrap.servers"] = KAFKA_BROKERS
        config["acks"] = "all"
        return KafkaProducer(config, keySerializer, valueSerializer)
    }

    fun <K, V> getTestConsumer(
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        topics:
        List<String>
    ): KafkaConsumer<K, V> {
        val config = Properties()
        config[ConsumerConfig.CLIENT_ID_CONFIG] = "integration-test-consumer-${(0..100).random()}"
        config[ConsumerConfig.GROUP_ID_CONFIG] = "integration-test-consumers-${(0..100).random()}"
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KAFKA_BROKERS
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        val consumer = KafkaConsumer(config, keyDeserializer, valueDeserializer)
        consumer.subscribe(topics)
        return consumer
    }
}