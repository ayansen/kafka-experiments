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
import ayansen.programming.kafka.experiments.Fixtures.generateSampleEvents
import ayansen.programming.kafka.experiments.Fixtures.getConsumerProperties
import ayansen.programming.kafka.experiments.Fixtures.getProducerProperties
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class EventOrderingIT {

    private val appConsumerTopic: String = "test_topic_v1"
    private val appProducerTopic: String = "test_topic_mirrored_v1"
    private val kafkaMirror = KafkaMirror(
        getConsumerProperties(),
        getProducerProperties(),
        appConsumerTopic,
        appProducerTopic
    )

    /**
     * This test is to validate that kafka records are in order for a particular partition
     */
    @Test
    fun `test ordering of events for a particular partition assigned based on record key`() {
        val firstSample = generateSampleEvents(10, "firstSample")
        val secondSample = generateSampleEvents(10, "secondSample")
        kafkaMirror.processRecords(500, 2)
        IntegrationTestUtils.produceSynchronously(
            false,
            appConsumerTopic,
            0,
            firstSample
        )
        IntegrationTestUtils.produceSynchronously(
            false,
            appConsumerTopic,
            1,
            secondSample
        )
        val consumedRecords =
            IntegrationTestUtils.waitUntilMinRecordsReceived<String, SampleEvent>(appProducerTopic, 20, 20000)

        val consumedFirstSampleEvents = consumedRecords
            .filter { it.value().eventName.contains("firstSample") }
            .sortedBy { it.timestamp() }
            .map { it.value() }

        val consumedSecondSampleEvents = consumedRecords
            .filter { it.value().eventName.contains("secondSample") }
            .sortedBy { it.timestamp() }
            .map { it.value() }
        assertEquals(consumedFirstSampleEvents, firstSample.map { it.value })
        assertEquals(consumedSecondSampleEvents, secondSample.map { it.value })
    }
}