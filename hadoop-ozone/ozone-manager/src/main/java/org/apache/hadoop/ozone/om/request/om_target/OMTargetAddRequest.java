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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TARGET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.FlatResource.TARGET_LOCK;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.TargetConfig;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.om_target.OMTargetAddResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddTargetRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

/**
 * Handles OM Target Add Request.
 * This request is used to add a target in the Ozone Manager.
 */
public class OMTargetAddRequest extends OMClientRequest {
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(OMTargetAddRequest.class);

  public OMTargetAddRequest(
      OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    AddTargetRequest addTargetRequest = getOmRequest().getAddTargetRequest();
    if (StringUtils.isBlank(addTargetRequest.getTargetId())) {
      addTargetRequest =
          addTargetRequest.toBuilder()
              .setTargetId(generateName())
              .build();

    }
    return getOmRequest().toBuilder().setAddTargetRequest(addTargetRequest).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    OMResponse.Builder omResponse = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AddTarget)
        .setStatus(OzoneManagerProtocolProtos.Status.OK);
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    Result result;


    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumAddTarget();

    boolean lockAcquired = false;
    AddTargetRequest addTargetRequest =
        getOmRequest().getAddTargetRequest();
    TargetConfig targetConfig = TargetConfig.fromProtobuf(addTargetRequest.getTargetConfig());
    String targetId = addTargetRequest.getTargetId();

    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(TARGET_LOCK, "target"));
      lockAcquired = getOmLockDetails().isLockAcquired();

      if (omMetadataManager.getTargetTable().isExist(targetId)) {
        LOG.debug("Target {} already exists.", targetId);
        throw new OMException("Target " + targetId + " already exists.", TARGET_ALREADY_EXISTS);
      }

      omMetadataManager.getTargetTable().addCacheEntry(
          new CacheKey<>(targetId),
          CacheValue.get(transactionLogIndex, targetConfig));

      omResponse.setSuccess(true);
      omResponse.setAddTargetResponse(OzoneManagerProtocolProtos.AddTargetResponse.newBuilder()
          .setResponse(true));
      omClientResponse = new OMTargetAddResponse(omResponse.build(), targetConfig);
      result = Result.SUCCESS;
    } catch (IOException ioe) {
      exception = ioe;
      result = Result.FAILURE;
      omClientResponse = new OMTargetAddResponse(createErrorOMResponse(omResponse, exception));
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(TARGET_LOCK, "target"));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    Map<String, String> auditMap = targetConfig.toAuditMap();
    if (result == Result.SUCCESS) {
      LOG.debug("Add target {} success.", targetId);
    } else {
      LOG.error("Add target {} failed.", targetId, exception);
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.ADD_TARGET, auditMap,
        exception, getOmRequest().getUserInfo()));


    return omClientResponse;
  }

  private static String generateName() {
    String timePattern = "yyyyMMdd-HHmmss.SSS";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
    Instant instant = Instant.ofEpochMilli(Time.now());
    return "t" + formatter.format(
        ZonedDateTime.ofInstant(instant, ZoneId.of("UTC")));
  }
}
