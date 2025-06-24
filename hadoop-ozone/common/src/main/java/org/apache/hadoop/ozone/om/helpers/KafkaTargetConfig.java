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

import java.util.List;

public class KafkaTargetConfig extends DefaultTargetConfig {

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

  public KafkaTargetConfig(String topic, List<String> endpoints, boolean isSaslEnabled,
                           String saslUsername, String saslPassword, String saslMechanism, boolean isTlsEnabled,
                           boolean tlsSkipVerify, String clientTlsCert, String clientTlsKey) {
    this.topic = topic;
    this.endpoints = endpoints;
    this.isSaslEnabled = isSaslEnabled;
    this.saslUsername = saslUsername;
    this.saslPassword = saslPassword;
    this.saslMechanism = saslMechanism;
    this.isTlsEnabled = isTlsEnabled;
    this.tlsSkipVerify = tlsSkipVerify;
    this.clientTlsCert = clientTlsCert;
    this.clientTlsKey = clientTlsKey;
  }

  public KafkaTargetConfig(String topic, List<String> endpoints) {
    this(topic, endpoints, false, null, null, null, false, false, null, null);
  }

  public KafkaTargetConfig(
      String topic, List<String> endpoints, String saslUsername,
      String saslPassword, String saslMechanism) {
    this(topic, endpoints, true, saslUsername, saslPassword, saslMechanism, false, false, null, null);
  }

  public KafkaTargetConfig(
      String topic, List<String> endpoints,
      boolean tlsSkipVerify, String clientTlsCert, String clientTlsKey) {
    this(topic, endpoints, false, null, null, null, true, tlsSkipVerify, clientTlsCert, clientTlsKey);
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
