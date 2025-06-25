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

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests for TargetConfig.
 */
public class TestTargetConfig {

  private static final String TARGET_ID = "test-target-id";
  private static final String TOPIC = "test-topic";
  private static final List<String> ENDPOINTS = Arrays.asList("endpoint1", "endpoint2");
  private static final boolean IS_SASL_ENABLED = true;
  private static final String SASL_USERNAME = "testuser";
  private static final String SASL_PASSWORD = "testpass";
  private static final String SASL_MECHANISM = "PLAIN";
  private static final boolean IS_TLS_ENABLED = true;
  private static final boolean TLS_SKIP_VERIFY = false;
  private static final String CLIENT_TLS_CERT = "cert";
  private static final String CLIENT_TLS_KEY = "key";

  @Test
  public void testGetTargetConfigFromProto() {
    String[] endpoints = new String[]{"endpoint1", "endpoint2"};

    OzoneManagerProtocolProtos.KafkaTargetConfig kafkaConfig =
        OzoneManagerProtocolProtos.KafkaTargetConfig.newBuilder()
        .addAllEndpoints(Arrays.asList(endpoints))
        .setTopic(TOPIC)
        .setIsSaslEnabled(IS_SASL_ENABLED)
        .setSaslUsername(SASL_USERNAME)
        .setSaslPassword(SASL_PASSWORD)
        .setSaslMechanism(SASL_MECHANISM)
        .setIsTlsEnabled(IS_TLS_ENABLED)
        .setTlsSkipVerify(TLS_SKIP_VERIFY)
        .setClientTlsCert(CLIENT_TLS_CERT)
        .setClientTlsKey(CLIENT_TLS_KEY)
        .build();

    OzoneManagerProtocolProtos.TargetConfig proto = OzoneManagerProtocolProtos.TargetConfig.newBuilder()
        .setType(Kafka)
        .setTargetId(TARGET_ID)
        .setKafkaTargetConfig(kafkaConfig)
        .build();

    TargetConfig config = TargetConfig.fromProtobuf(proto);

    assertEquals(OzoneManagerProtocolProtos.TargetConfig.Type.Kafka, config.getType());
    assertInstanceOf(KafkaTargetConfig.class, config);
    KafkaTargetConfig kafkaTargetConfig = (KafkaTargetConfig) config;
    assertEquals(TOPIC, kafkaTargetConfig.getTopic());
    assertEquals(IS_SASL_ENABLED, kafkaTargetConfig.isSaslEnabled());
    assertEquals(SASL_USERNAME, kafkaTargetConfig.getSaslUsername());
    assertEquals(SASL_PASSWORD, kafkaTargetConfig.getSaslPassword());
    assertEquals(SASL_MECHANISM, kafkaTargetConfig.getSaslMechanism());
    assertEquals(IS_TLS_ENABLED, kafkaTargetConfig.isTlsEnabled());
    assertEquals(TLS_SKIP_VERIFY, kafkaTargetConfig.isTlsSkipVerify());
    assertEquals(CLIENT_TLS_CERT, kafkaTargetConfig.getClientTlsCert());
    assertEquals(CLIENT_TLS_KEY, kafkaTargetConfig.getClientTlsKey());
  }

  @Test
  public void testToProto() {

    KafkaTargetConfig kafkaConfig = KafkaTargetConfig.newBuilder()
        .setTargetId(TARGET_ID)
        .setType(Kafka)
        .setTopic(TOPIC)
        .setEndpoints(ENDPOINTS)
        .setIsSaslEnabled(IS_SASL_ENABLED)
        .setSaslUsername(SASL_USERNAME)
        .setSaslPassword(SASL_PASSWORD)
        .setSaslMechanism(SASL_MECHANISM)
        .setIsTlsEnabled(IS_TLS_ENABLED)
        .setTlsSkipVerify(TLS_SKIP_VERIFY)
        .setClientTlsCert(CLIENT_TLS_CERT)
        .setClientTlsKey(CLIENT_TLS_KEY)
        .build();

    OzoneManagerProtocolProtos.TargetConfig proto = kafkaConfig.toProtobuf();

    assertEquals(Kafka, proto.getType());
    assertEquals(TARGET_ID, proto.getTargetId());
    OzoneManagerProtocolProtos.KafkaTargetConfig kafkaProto =
        proto.getKafkaTargetConfig();
    assertEquals(TOPIC, kafkaProto.getTopic());
    assertEquals(IS_SASL_ENABLED, kafkaProto.getIsSaslEnabled());
    assertEquals(SASL_USERNAME, kafkaProto.getSaslUsername());
    assertEquals(SASL_PASSWORD, kafkaProto.getSaslPassword());
    assertEquals(SASL_MECHANISM, kafkaProto.getSaslMechanism());
    assertEquals(IS_TLS_ENABLED, kafkaProto.getIsTlsEnabled());
    assertEquals(TLS_SKIP_VERIFY, kafkaProto.getTlsSkipVerify());
    assertEquals(CLIENT_TLS_CERT, kafkaProto.getClientTlsCert());
    assertEquals(CLIENT_TLS_KEY, kafkaProto.getClientTlsKey());
  }
}
