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

package org.apache.hadoop.ozone.om.request.bucket.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.S3NotificationInfo;
import org.apache.hadoop.ozone.om.helpers.S3NotificationInfo.EventType;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.bucket.TestBucketRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.Test;

/**
 * Tests bucket set notification request.
 */
public class TestOMBucketSetNotificationRequest extends TestBucketRequest {

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "owner";

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    S3NotificationInfo s3NotificationInfo = new S3NotificationInfo("target", EventType.S3_OBJECT_CREATE_PUT);
    List<S3NotificationInfo> s3NotificationInfos = Lists.newArrayList(s3NotificationInfo);

    OMRequest originalRequest =
        OMRequestTestUtils.createBucketSetNotificationRequest(volumeName, bucketName, s3NotificationInfos);
    OMBucketSetNotificationRequest omBucketSetNotificationRequest = new OMBucketSetNotificationRequest(originalRequest);

    OMClientResponse omClientResponse = omBucketSetNotificationRequest.validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetNotificationResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<S3NotificationInfo> bucketNotificationInfoList = omMetadataManager.getBucketTable()
        .get(bucketKey).getS3NotificationInfos();

    assertEquals(s3NotificationInfos.size(), bucketNotificationInfoList.size());
    assertEquals(s3NotificationInfo, bucketNotificationInfoList.get(0));
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    S3NotificationInfo s3NotificationInfo = new S3NotificationInfo("target", EventType.S3_OBJECT_CREATE_PUT);
    List<S3NotificationInfo> s3NotificationInfos = Lists.newArrayList(s3NotificationInfo);

    OMRequest originalRequest =
        OMRequestTestUtils.createBucketSetNotificationRequest(volumeName, bucketName, s3NotificationInfos);
    OMBucketSetNotificationRequest omBucketSetNotificationRequest = new OMBucketSetNotificationRequest(originalRequest);

    OMClientResponse omClientResponse = omBucketSetNotificationRequest.validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetNotificationResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND, omResponse.getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithEmptyNotificationList() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    List<S3NotificationInfo> s3NotificationInfos = Lists.newArrayList();

    OMRequest originalRequest =
        OMRequestTestUtils.createBucketSetNotificationRequest(volumeName, bucketName, s3NotificationInfos);
    OMBucketSetNotificationRequest omBucketSetNotificationRequest = new OMBucketSetNotificationRequest(originalRequest);

    OMClientResponse omClientResponse = omBucketSetNotificationRequest.validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetNotificationResponse());
    assertEquals(Status.BUCKET_NOT_FOUND, omResponse.getStatus());
  }
}
