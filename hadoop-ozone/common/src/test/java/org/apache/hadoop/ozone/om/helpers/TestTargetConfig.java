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

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig.Type.Kafka;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests for TargetConfig.
 */
public class TestTargetConfig {

  @Test
  public void testGetTargetConfigFromProto() {
    String[] endpoints = new String[]{"endpoint1", "endpoint2"};
    String topic = "test-topic";
    boolean isSaslEnabled = true;
    String saslUsername = "testuser";
    String saslPassword = "testpass";
    String saslMechanism = "PLAIN";
    boolean isTlsEnabled = true;
    boolean tlsSkipVerify = false;
    String clientTlsCert = "cert";
    String clientTlsKey = "key";

    OzoneManagerProtocolProtos.KafkaTargetConfig kafkaConfig =
        OzoneManagerProtocolProtos.KafkaTargetConfig.newBuilder()
        .addAllEndpoints(java.util.Arrays.asList(endpoints))
        .setTopic(topic)
        .setIsSaslEnabled(isSaslEnabled)
        .setSaslUsername(saslUsername)
        .setSaslPassword(saslPassword)
        .setSaslMechanism(saslMechanism)
        .setIsTlsEnabled(isTlsEnabled)
        .setTlsSkipVerify(tlsSkipVerify)
        .setClientTlsCert(clientTlsCert)
        .setClientTlsKey(clientTlsKey)
        .build();

    OzoneManagerProtocolProtos.TargetConfig proto = OzoneManagerProtocolProtos.TargetConfig.newBuilder()
        .setType(Kafka)
        .setKafkaTargetConfig(kafkaConfig)
        .build();

    TargetConfig config = TargetConfig.fromProtobuf(proto);

    assertEquals(OzoneManagerProtocolProtos.TargetConfig.Type.Kafka, config.getType());
    assertInstanceOf(KafkaTargetConfig.class, config);
    KafkaTargetConfig kafkaTargetConfig = (KafkaTargetConfig) config;
    assertEquals(topic, kafkaTargetConfig.getTopic());
    assertEquals(isSaslEnabled, kafkaTargetConfig.isSaslEnabled());
    assertEquals(saslUsername, kafkaTargetConfig.getSaslUsername());
    assertEquals(saslPassword, kafkaTargetConfig.getSaslPassword());
    assertEquals(saslMechanism, kafkaTargetConfig.getSaslMechanism());
    assertEquals(isTlsEnabled, kafkaTargetConfig.isTlsEnabled());
    assertEquals(tlsSkipVerify, kafkaTargetConfig.isTlsSkipVerify());
    assertEquals(clientTlsCert, kafkaTargetConfig.getClientTlsCert());
    assertEquals(clientTlsKey, kafkaTargetConfig.getClientTlsKey());
  }

  @Test
  public void testToProto() {
    List<String> endpoints = new ArrayList<>();
    endpoints.add("endpoint1");
    endpoints.add("endpoint2");
    String topic = "test-topic";
    boolean isSaslEnabled = true;
    String saslUsername = "testuser";
    String saslPassword = "testpass";
    String saslMechanism = "PLAIN";
    boolean isTlsEnabled = true;
    boolean tlsSkipVerify = false;
    String clientTlsCert = "cert";
    String clientTlsKey = "key";

    KafkaTargetConfig kafkaConfig = KafkaTargetConfig.newBuilder()
            .setTopic(topic)
            .setEndpoints(endpoints)
            .setIsSaslEnabled(isSaslEnabled)
            .setSaslUsername(saslUsername)
            .setSaslPassword(saslPassword)
            .setSaslMechanism(saslMechanism)
            .setIsTlsEnabled(isTlsEnabled)
            .setTlsSkipVerify(tlsSkipVerify)
            .setClientTlsCert(clientTlsCert)
            .setClientTlsKey(clientTlsKey)
            .build();

    OzoneManagerProtocolProtos.TargetConfig proto = kafkaConfig.toProtobuf();

    assertEquals(Kafka, proto.getType());

    OzoneManagerProtocolProtos.KafkaTargetConfig kafkaProto =
        proto.getKafkaTargetConfig();
    assertEquals(topic, kafkaProto.getTopic());
    assertEquals(isSaslEnabled, kafkaProto.getIsSaslEnabled());
    assertEquals(saslUsername, kafkaProto.getSaslUsername());
    assertEquals(saslPassword, kafkaProto.getSaslPassword());
    assertEquals(saslMechanism, kafkaProto.getSaslMechanism());
    assertEquals(isTlsEnabled, kafkaProto.getIsTlsEnabled());
    assertEquals(tlsSkipVerify, kafkaProto.getTlsSkipVerify());
    assertEquals(clientTlsCert, kafkaProto.getClientTlsCert());
    assertEquals(clientTlsKey, kafkaProto.getClientTlsKey());
  }
}
