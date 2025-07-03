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

package org.apache.hadoop.ozone.om.request.om_target;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.KafkaTargetConfig;
import org.apache.hadoop.ozone.om.helpers.TargetConfig;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TargetConfig.Type;
import org.junit.jupiter.api.Test;

/**
 * Test class for OMTargetAddRequest.
 */
public class TestOMTargetAddRequest extends TestOMTarget {

  private final TargetConfig kafkaTargetConfig = KafkaTargetConfig.newBuilder()
      .setTargetId("")
      .setTopic("testTopic")
      .setEndpoints(new ArrayList<>())
      .build();

  @Test
  public void testPreExecute() throws Exception {

    OMRequest request = OMRequestTestUtils.createTargetAddRequest(kafkaTargetConfig);
    OMTargetAddRequest targetAddRequest =
        new OMTargetAddRequest(request);
    OMRequest preExecuteRequest = targetAddRequest.preExecute(ozoneManager);

    assertNotEquals(request, preExecuteRequest);
    assertFalse(StringUtils.isBlank(preExecuteRequest.getAddTargetRequest().getTargetId()));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {

    OMRequest request = OMRequestTestUtils.createTargetAddRequest(kafkaTargetConfig);
    OMTargetAddRequest targetAddRequest =
        new OMTargetAddRequest(request);

    targetAddRequest.preExecute(ozoneManager);
    // Validate and update cache
    OMClientResponse omClientResponse = targetAddRequest.validateAndUpdateCache(ozoneManager, 1L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getAddTargetResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    String targetId = targetAddRequest.getOmRequest().getAddTargetRequest().getTargetId();
    TargetConfig targetConfig = omMetadataManager.getTargetTable().get(targetId);
    assertNotNull(targetConfig);

    assertEquals(Type.Kafka, targetConfig.getType());
    assertInstanceOf(KafkaTargetConfig.class, targetConfig);

    KafkaTargetConfig kafkaConfig = (KafkaTargetConfig) targetConfig;
    assertEquals("testTopic", kafkaConfig.getTopic());
    assertEquals(0, kafkaConfig.getEndpoints().size());

  }

}
