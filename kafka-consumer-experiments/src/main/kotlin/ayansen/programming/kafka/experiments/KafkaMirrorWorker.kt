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

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.lang.Thread.sleep
import java.time.Duration
import java.util.*
import kotlin.system.exitProcess

class KafkaMirrorWorker(
    consumerConfig: Properties,
    producerConfig: Properties,
    consumerTopic: String,
    private val producerTopic: String
) {
    companion object {
        private const val POLL_INTERVAL_IN_MS = 1000L
    }

    private val consumer: KafkaConsumer<String, Any> = KafkaConsumer(consumerConfig)
    private val producer: KafkaProducer<String, Any> = KafkaProducer(producerConfig)

    init {
        consumer.subscribe(listOf(consumerTopic))
    }
    fun processRecordsWithDelay( delayInMs: Long) {
        try {
            while (true) {
                consumer.poll(Duration.ofMillis(POLL_INTERVAL_IN_MS)).map {
                    sleep(delayInMs)
                    ProducerRecord(producerTopic, it.key(), it.value())
                }.forEach {
                    producer.send(it)
                }
            }
        } catch (ex: Exception) {
            exitProcess(1)
        }
    }
}