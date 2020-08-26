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

import ayansen.programming.kafka.experiments.Fixtures.APP_GROUP_ID
import ayansen.programming.kafka.experiments.Fixtures.KAFKA_BROKERS
import ayansen.programming.kafka.experiments.Fixtures.SCHEMA_REGISTRY_URL
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class KafkaMirrorIT {
    private val consumerProperties: Properties = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KAFKA_BROKERS,
        ConsumerConfig.GROUP_ID_CONFIG to APP_GROUP_ID,
        "schema.registry.url" to SCHEMA_REGISTRY_URL
    ).toProperties()
    private val producerProperties: Properties = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KAFKA_BROKERS,
        "schema.registry.url" to SCHEMA_REGISTRY_URL
    ).toProperties()
    private val kafkaAdminClient = KafkaAdminClient.create(consumerProperties)
    private val appConsumerTopic: String = "test_topic_v1"
    private val appProducerTopic: String = "test_topic_mirrored_v1"
    private val kafkaMirror = KafkaMirror(
        consumerProperties,
        producerProperties,
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
        val firstSample = Fixtures.generateSampleEvents(10, "firstSample")
        val secondSample = Fixtures.generateSampleEvents(10, "secondSample")
        kafkaMirror.processRecords(500, 2)
        val testProducer = Fixtures.getTestProducer(
            StringSerializer(),
            Utils.getAvroSerializer(SCHEMA_REGISTRY_URL)
        )
        val testConsumer = Fixtures.getTestConsumer(
            StringDeserializer(),
            Utils.getAvroDeserializer(SCHEMA_REGISTRY_URL),
            listOf(appProducerTopic)
        )
        firstSample.forEach {
            testProducer.send(
                ProducerRecord(
                    appConsumerTopic,
                    1,
                    it.eventName.toString(),
                    it
                )
            )
        }
        firstSample.forEach {
            testProducer.send(
                ProducerRecord(
                    appConsumerTopic,
                    2,
                    it.eventName.toString(),
                    it
                )
            )
        }
        val consumedRecords = mutableListOf<ConsumerRecord<String, Any>>()
        while (consumedRecords.size == 20) {
            consumedRecords.addAll(testConsumer.poll(Duration.ofMillis(5000)).records(appProducerTopic))
        }
    }
}