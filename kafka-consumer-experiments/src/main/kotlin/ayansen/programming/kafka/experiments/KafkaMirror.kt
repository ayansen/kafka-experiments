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

import java.util.Properties
import kotlin.concurrent.thread

class KafkaMirror(
   private val consumerConfig: Properties,
    private val producerConfig: Properties,
    private val consumerTopic: String,
    private val producerTopic: String
){
    fun processRecords(delayInMs: Long, workerThreadPool:Int){
        repeat(workerThreadPool) {
            thread {
                KafkaMirrorWorker(
                    consumerConfig = consumerConfig,
                    producerConfig = producerConfig,
                    consumerTopic = consumerTopic,
                    producerTopic = producerTopic
                ).processRecordsWithDelay(delayInMs)
            }
        }
    }
}