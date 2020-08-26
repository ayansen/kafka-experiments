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
import ayansen.programming.kafka.experiments.Fixtures.APP_GROUP_ID
import ayansen.programming.kafka.experiments.Fixtures.generateSampleEvents
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

class KafkaMirrorIT {

    private val kafkaAdminClient = KafkaAdminClient.create(getConsumerProperties())
    private val appConsumerTopic: String = "test_topic_v1"
    private val appProducerTopic: String = "test_topic_mirrored_v1"
    private val kafkaMirror = KafkaMirror(
        getConsumerProperties(),
        getProducerProperties(),
        appConsumerTopic,
        appProducerTopic
    )

    @BeforeEach
    fun setup() {
        kafkaAdminClient.createTopics(
            listOf(
                NewTopic(appConsumerTopic, 2, 1),
                NewTopic(appProducerTopic, 2, 1)
            )
        )
    }

    @AfterEach
    fun destroy() {
        kafkaAdminClient.deleteConsumerGroups(listOf(APP_GROUP_ID))
        kafkaAdminClient.deleteTopics(
            listOf(
                appConsumerTopic,
                appProducerTopic
            )
        )
    }

    /**
     * This test is that kafka records are in order for a perticular partition
     */
    @Test
    fun `test ordering of events for a particular partition assigned based on record key`() {
        val firstSample = generateSampleEvents(10, "firstSample")
        val secondSample = generateSampleEvents(10, "secondSample")
        kafkaMirror.processRecords(500, 2)
        IntegrationTestUtils.produceSynchronously(
            false,
            appConsumerTopic,
            1,
            firstSample
        )
        IntegrationTestUtils.produceSynchronously(
            false,
            appConsumerTopic,
            2,
            secondSample
        )
        val consumedRecords =
            IntegrationTestUtils.waitUntilMinRecordsReceived<String, SampleEvent>(appProducerTopic, 20, 20000)
        val consumedFirstSampleEvents =
            consumedRecords.filter { it.value().eventName == "firstSample" }.sortedBy { it.timestamp() }
                .map { it.value() }
        val consumedSecondSampleEvents =
            consumedRecords.filter { it.value().eventName == "secondSample" }.sortedBy { it.timestamp() }
                .map { it.value() }
        assertEquals(consumedFirstSampleEvents, firstSample.map { it.value })
        assertEquals(consumedSecondSampleEvents, secondSample.map { it.value })
    }

    private fun getConsumerProperties(): Properties {
        val config = Properties()
        config[ConsumerConfig.GROUP_ID_CONFIG] = APP_GROUP_ID
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "true"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        return config
    }

    private fun getProducerProperties(): Properties {
        val config = Properties()
        config["client.id"] = Fixtures.CLIENT_ID
        config["bootstrap.servers"] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config["acks"] = "all"
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        return config
    }
}