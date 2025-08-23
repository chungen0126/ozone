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

import java.util.List;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig.Type;

/**
 * Configuration for Kafka target.
 */
public final class KafkaTargetConfig extends TargetConfig {

  private final String topic;
  private final List<String> endpoints;
  private final boolean isSaslEnabled;
  private final String saslUsername;
  private final String saslPassword;
  private final String saslMechanism;
  private final boolean isTlsEnabled;
  private final boolean tlsSkipVerify;
  private final String clientTlsCert;
  private final String clientTlsKey;

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public OzoneManagerProtocolProtos.TargetConfig toProtobuf() {
    return OzoneManagerProtocolProtos.TargetConfig.newBuilder()
        .setType(getType())
        .setTargetId(getTargetId())
        .setKafkaTargetConfig(OzoneManagerProtocolProtos.KafkaTargetConfig.newBuilder()
            .addAllEndpoints(endpoints)
            .setTopic(topic)
            .setIsSaslEnabled(isSaslEnabled)
            .setSaslUsername(saslUsername)
            .setSaslPassword(saslPassword)
            .setSaslMechanism(saslMechanism)
            .setIsTlsEnabled(isTlsEnabled)
            .setTlsSkipVerify(tlsSkipVerify)
            .setClientTlsCert(clientTlsCert)
            .setClientTlsKey(clientTlsKey)
            .build())
        .build();
  }

  /**
   * Builder for KafkaTargetConfig.
   */
  public static class Builder {
    private String targetId;
    private Type type = Kafka;
    private String topic;
    private List<String> endpoints;
    private boolean isSaslEnabled;
    private String saslUsername;
    private String saslPassword;
    private String saslMechanism;
    private boolean isTlsEnabled;
    private boolean tlsSkipVerify;
    private String clientTlsCert;
    private String clientTlsKey;

    public Builder setTargetId(String targetId) {
      this.targetId = targetId;
      return this;
    }

    public Builder setType(Type type) {
      this.type = type;
      return this;
    }

    public Builder setTopic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder setEndpoints(List<String> endpoints) {
      this.endpoints = endpoints;
      return this;
    }

    public Builder setIsSaslEnabled(boolean isSaslEnabled) {
      this.isSaslEnabled = isSaslEnabled;
      return this;
    }

    public Builder setSaslUsername(String saslUsername) {
      this.saslUsername = saslUsername;
      return this;
    }

    public Builder setSaslPassword(String saslPassword) {
      this.saslPassword = saslPassword;
      return this;
    }

    public Builder setSaslMechanism(String saslMechanism) {
      this.saslMechanism = saslMechanism;
      return this;
    }

    public Builder setIsTlsEnabled(boolean isTlsEnabled) {
      this.isTlsEnabled = isTlsEnabled;
      return this;
    }

    public Builder setTlsSkipVerify(boolean tlsSkipVerify) {
      this.tlsSkipVerify = tlsSkipVerify;
      return this;
    }

    public Builder setClientTlsCert(String clientTlsCert) {
      this.clientTlsCert = clientTlsCert;
      return this;
    }

    public Builder setClientTlsKey(String clientTlsKey) {
      this.clientTlsKey = clientTlsKey;
      return this;
    }

    public KafkaTargetConfig build() {
      return new KafkaTargetConfig(this);
    }
  }

  private KafkaTargetConfig(Builder builder) {
    super(builder.type, builder.targetId);
    this.topic = builder.topic;
    this.endpoints = builder.endpoints;
    this.isSaslEnabled = builder.isSaslEnabled;
    this.saslUsername = builder.saslUsername;
    this.saslPassword = builder.saslPassword;
    this.saslMechanism = builder.saslMechanism;
    this.isTlsEnabled = builder.isTlsEnabled;
    this.tlsSkipVerify = builder.tlsSkipVerify;
    this.clientTlsCert = builder.clientTlsCert;
    this.clientTlsKey = builder.clientTlsKey;
  }

  public String getTopic() {
    return topic;
  }

  public List<String> getEndpoints() {
    return endpoints;
  }

  public String getSaslUsername() {
    return saslUsername;
  }

  public String getSaslPassword() {
    return saslPassword;
  }

  public String getSaslMechanism() {
    return saslMechanism;
  }

  public boolean isTlsSkipVerify() {
    return tlsSkipVerify;
  }

  public String getClientTlsCert() {
    return clientTlsCert;
  }

  public String getClientTlsKey() {
    return clientTlsKey;
  }
  
  public boolean isTlsEnabled() {
    return isTlsEnabled;
  }

  public boolean isSaslEnabled() {
    return isSaslEnabled;
  }
}
