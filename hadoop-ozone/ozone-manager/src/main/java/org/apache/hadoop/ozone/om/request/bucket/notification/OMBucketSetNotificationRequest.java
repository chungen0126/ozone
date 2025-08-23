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

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.S3NotificationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.notification.OmBucketNotificationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse.Builder;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetNotificationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetNotificationResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle set Notification request for bucket.
 */
public class OMBucketSetNotificationRequest extends OMBucketNotificationRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketSetNotificationRequest.class);

  public OMBucketSetNotificationRequest(OMRequest omRequest) {
    super(omRequest, (bucketInfo, s3NotificationInfos) -> bucketInfo.setS3NotificationInfos(s3NotificationInfos),
        omRequest.getSetNotificationRequest().getVolumeName(),
        omRequest.getSetNotificationRequest().getBucketName(),
        omRequest.getSetNotificationRequest().getNotificationInfoList()
            .stream().map(S3NotificationInfo::fromProtobuf).collect(
            Collectors.toList()));
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    long modificationTime = Time.now();
    SetNotificationRequest.Builder setNotificationRequest =
        getOmRequest().getSetNotificationRequest().toBuilder()
            .setModificationTime(modificationTime);
    return getOmRequest().toBuilder()
        .setSetNotificationRequest(setNotificationRequest)
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  OMClientResponse onSuccess(Builder omResponse, OmBucketInfo omBucketInfo, boolean operationResult) {
    omResponse.setSuccess(operationResult);
    omResponse.setSetNotificationResponse(SetNotificationResponse.newBuilder().setSuccess(operationResult));
    return new OmBucketNotificationResponse(omResponse.build(), omBucketInfo);
  }

  @Override
  void onComplete(boolean operationResult, Exception exception, OMMetrics omMetrics, AuditLogger auditLogger,
                  Map<String, String> auditMap) {
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_S3_NOTIFICATION, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (operationResult) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set notification: {} for volume: {}, bucket: {} success!",
            getS3NotificationInfo(), getVolumeName(), getBucketName());
      }
    } else {
      omMetrics.incNumBucketUpdateFails();
      if (exception == null) {
        LOG.error("Set notification: {} for volume: {}, bucket: {} failed!",
            getS3NotificationInfo(), getVolumeName(), getBucketName());
      } else {
        LOG.error("Set notification: {} for volume: {}, bucket: {} failed!",
            getS3NotificationInfo(), getVolumeName(), getBucketName(), exception);
      }
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    ozoneManager.getMetrics().incNumSetNotification();
    return super.validateAndUpdateCache(ozoneManager, context);
  }
}
