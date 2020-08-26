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

data class KeyValueTimestamp<T, U>(val key: T, val value: U, val timestamp: Long)


object Fixtures {

    const val KAFKA_BROKERS = "localhost:19092"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8083"
    const val APP_GROUP_ID = "test_application"
    const val CLIENT_ID = "test_client"

    fun generateSampleEvents(numberOfEvents: Int, eventName: String): List<KeyValueTimestamp<String, SampleEvent>> =
        (1..numberOfEvents).map {
            val event = SampleEvent.newBuilder()
                .setId(it.toString())
                .setEventName(eventName)
                .build()
            KeyValueTimestamp(eventName, event, it * 100L)
        }
}