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

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig.Type;

/**
 * Default configuration for a target in Ozone Manager.
 */
public abstract class TargetConfig {
  private static final Codec<TargetConfig> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.TargetConfig.getDefaultInstance()),
      TargetConfig::fromProtobuf,
      TargetConfig::toProtobuf,
      TargetConfig.class);

  private final Type type;
  private final String targetId;

  public TargetConfig(Type type, String targetId) {
    this.type = type;
    this.targetId = targetId;
  }

  public static Codec<TargetConfig> getCodec() {
    return CODEC;
  }

  public Type getType() {
    return type;
  }

  public String getTargetId() {
    return targetId;
  }

  public static TargetConfig fromProtobuf(
      OzoneManagerProtocolProtos.TargetConfig proto) {
    switch (proto.getType()) {
    case Kafka:
      return KafkaTargetConfig.newBuilder()
          .setTargetId(proto.getTargetId())
          .setType(proto.getType())
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

  public abstract OzoneManagerProtocolProtos.TargetConfig toProtobuf();
}
