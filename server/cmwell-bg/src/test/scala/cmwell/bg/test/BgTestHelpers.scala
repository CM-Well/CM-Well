/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package cmwell.bg.test

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object BgTestHelpers {
  def kafkaProducer(bootstrapServers: String)= {
    val producerProperties = new Properties
    producerProperties.put("bootstrap.servers", bootstrapServers)
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
  }

}
