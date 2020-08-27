package ayansen.programming.kafka.experiments


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
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import kotlin.test.assertEquals


/**
 * Utility functions to make integration testing more convenient.
 */
object IntegrationTestUtils {

    fun <V, K> produceSynchronously(
        eos: Boolean,
        topic: String,
        partition: Int?,
        toProduce: List<KeyValueTimestamp<K, V>>
    ) {
        getTestProducer<K,V>().use { producer ->
            if (eos) {
                producer.initTransactions()
                producer.beginTransaction()
            }
            val futures = mutableListOf<Future<RecordMetadata>>()
            for (record in toProduce) {
                val f = producer.send(
                    ProducerRecord(
                        topic,
                        partition,
                        record.timestamp,
                        record.key,
                        record.value,
                        null
                    )
                )
                futures.add(f)
            }
            if (eos) {
                producer.commitTransaction()
            } else {
                producer.flush()
            }
            for (future in futures) {
                try {
                    future.get()
                } catch (e: InterruptedException) {
                    throw RuntimeException(e)
                } catch (e: ExecutionException) {
                    throw RuntimeException(e)
                }
            }
        }
    }


    /**
     * Wait until enough data (consumer records) has been consumed.
     *
     * @param consumerConfig      Kafka Consumer configuration
     * @param topic               Kafka topic to consume from
     * @param expectedNumRecords  Minimum number of expected records
     * @param waitTime            Upper bound of waiting time in milliseconds
     * @param <K>                 Key type of the data records
     * @param <V>                 Value type of the data records
     * @return All the records consumed, or null if no records are consumed
    </V></K> */
    @Throws(Exception::class)
    fun <K, V> waitUntilMinRecordsReceived(
        topic: String?,
        expectedNumRecords: Int,
        waitTime: Long
    ): List<ConsumerRecord<K, V>> {

        val reason = String.format(
            "Did not receive all %d records from topic %s within %d ms",
            expectedNumRecords,
            topic,
            waitTime
        )
        getTestConsumer<K,V>().use { consumer ->
            val readData: List<ConsumerRecord<K, V>> =
                readRecords(topic, consumer, waitTime, expectedNumRecords)
            assertEquals(expectedNumRecords, readData.size, reason)
            return readData
        }
    }

    private fun <K, V> readRecords(
        topic: String?,
        consumer: Consumer<K, V>,
        waitTime: Long,
        maxMessages: Int
    ): List<ConsumerRecord<K, V>> {
        val consumerRecords: MutableList<ConsumerRecord<K, V>>
        consumer.subscribe(listOf(topic))
        val pollIntervalMs = 100
        consumerRecords = ArrayList()
        var totalPollTimeMs = 0
        while (totalPollTimeMs < waitTime &&
            continueConsuming(consumerRecords.size, maxMessages)
        ) {
            totalPollTimeMs += pollIntervalMs
            val records = consumer.poll(Duration.ofMillis(pollIntervalMs.toLong()))
            for (record in records!!) {
                consumerRecords.add(record)
            }
        }
        return consumerRecords.toList()
    }


    private fun continueConsuming(messagesConsumed: Int, maxMessages: Int): Boolean {
        return maxMessages <= 0 || messagesConsumed < maxMessages
    }

    private fun <K, V> getTestProducer(): KafkaProducer<K, V> {
        val config = Properties()
        config["bootstrap.servers"] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config["acks"] = "all"
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        return KafkaProducer(config)
    }

    private fun <K, V> getTestConsumer(): KafkaConsumer<K, V> {
        val config = Properties()
        config[ConsumerConfig.CLIENT_ID_CONFIG] = "integration-test-consumer-${(0..100).random()}"
        config[ConsumerConfig.GROUP_ID_CONFIG] = "integration-test-consumers-${(0..100).random()}"
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = Fixtures.KAFKA_BROKERS
        config["schema.registry.url"] = Fixtures.SCHEMA_REGISTRY_URL
        config[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "true"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        return KafkaConsumer(config)
    }
}