/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig.Type;

/**
 * Default configuration for a target in Ozone Manager.
 */
public class DefaultTargetConfig {

  public Type getType() {
    return Type.Kafka;
  }

  public static DefaultTargetConfig fromProtobuf(
      TargetConfig proto) {
    switch (proto.getType()) {
    case Kafka:
      return KafkaTargetConfig.newbBuilder()
          .setTopic(proto.getKafkaTargetConfig().getTopic())
          .setEndpoints(proto.getKafkaTargetConfig().getEndpointsList())
          .setIsSaslEnabled(proto.getKafkaTargetConfig().getIsSaslEnabled())
          .setSaslUsername(proto.getKafkaTargetConfig().getSaslUsername())
          .setSaslPassword(proto.getKafkaTargetConfig().getSaslPassword())
          .setSaslMechanism(proto.getKafkaTargetConfig().getSaslMechanism())
          .setIsTlsEnabled(proto.getKafkaTargetConfig().getIsTlsEnabled())
          .setTlsSkipVerify(proto.getKafkaTargetConfig().getTlsSkipVerify())
          .setClientTlsCert(proto.getKafkaTargetConfig().getClientTlsCert())
          .setClientTlsKey(proto.getKafkaTargetConfig().getClientTlsKey())
          .build();
    default:
      throw new IllegalArgumentException("Unknown target type: " + proto.getType());
    }
  }

  public TargetConfig toProtobuf() {
    Type type = getType();
    switch (type) {
    case Kafka:
      KafkaTargetConfig kafkaTargetConfig = (KafkaTargetConfig)this;
      return TargetConfig.newBuilder()
          .setType(type)
          .setKafkaTargetConfig(OzoneManagerProtocolProtos.KafkaTargetConfig.newBuilder()
              .addAllEndpoints(kafkaTargetConfig.getEndpoints())
              .setTopic(kafkaTargetConfig.getTopic())
              .setIsSaslEnabled(kafkaTargetConfig.isSaslEnabled())
              .setSaslUsername(kafkaTargetConfig.getSaslUsername())
              .setSaslPassword(kafkaTargetConfig.getSaslPassword())
              .setSaslMechanism(kafkaTargetConfig.getSaslMechanism())
              .setIsTlsEnabled(kafkaTargetConfig.isTlsEnabled())
              .setTlsSkipVerify(kafkaTargetConfig.isTlsSkipVerify())
              .setClientTlsCert(kafkaTargetConfig.getClientTlsCert())
              .setClientTlsKey(kafkaTargetConfig.getClientTlsKey()))
          .build();
    default:
      throw new IllegalArgumentException("Unknown target type: " + getType());
    }
  }
}
