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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.S3NotificationInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.notification.OmBucketNotificationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

/**
 * Base class for Bucket Notification request.
 */
public abstract class OmBucketNotificationRequest extends OMClientRequest {

  private final BiPredicate<OmBucketInfo, List<S3NotificationInfo>> bucketNotificationOp;
  private final String volumeName;
  private final String bucketName;
  private final List<S3NotificationInfo> s3NotificationInfo;

  public OmBucketNotificationRequest(
      OMRequest omRequest, BiPredicate<OmBucketInfo, List<S3NotificationInfo>> bucketNotificationOp,
      String volumeName, String bucketName, List<S3NotificationInfo> s3NotificationInfo) {
    super(omRequest);
    this.bucketNotificationOp = bucketNotificationOp;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.s3NotificationInfo = s3NotificationInfo;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();
    OmBucketInfo omBucketInfo;

    OMResponse.Builder omResponse = getResBuilder();
    OMClientResponse omClientResponse = null;
    Exception exception = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean lockAcquired = false;
    String volume = null;
    String bucket = null;
    boolean operationResult = false;
    try {
      ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(Pair.of(volumeName, bucketName));
      volume = resolvedBucket.realVolume();
      bucket = resolvedBucket.realBucket();

      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, bucket, null);
      }
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volume,
              bucket));
      lockAcquired = getOmLockDetails().isLockAcquired();

      String dbBucketKey = omMetadataManager.getBucketKey(volume, bucket);
      omBucketInfo = omMetadataManager.getBucketTable().get(dbBucketKey);
      if (omBucketInfo == null) {
        throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      operationResult = bucketNotificationOp.test(omBucketInfo, s3NotificationInfo);
      omBucketInfo.setUpdateID(transactionLogIndex);

      if (operationResult) {
        // Update the modification time when updating Notifications of Bucket.
        long modificationTime = omBucketInfo.getModificationTime();
        if (getOmRequest().getAddAclRequest().hasModificationTime()) {
          modificationTime = getOmRequest().getAddAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getSetAclRequest().hasModificationTime()) {
          modificationTime = getOmRequest().getSetAclRequest()
              .getModificationTime();
        } else if (getOmRequest().getRemoveAclRequest().hasModificationTime()) {
          modificationTime = getOmRequest().getRemoveAclRequest()
              .getModificationTime();
        }
        omBucketInfo = omBucketInfo.toBuilder()
            .setModificationTime(modificationTime).build();

        // update cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(dbBucketKey),
            CacheValue.get(transactionLogIndex, omBucketInfo));
      }

      omClientResponse = onSuccess(omResponse, omBucketInfo, operationResult);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = onFailure(omResponse, exception);
    } finally {
      if (lockAcquired) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volume,
                bucket));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    Map<String, String> auditMap = toAuditMap();
    if (s3NotificationInfo != null) {
      auditMap.put(OzoneConsts.S3_NOTIFICATION_INFO, s3NotificationInfo.toString());
    }

    onComplete(operationResult, exception, ozoneManager.getMetrics(),
        ozoneManager.getAuditLogger(), auditMap);
    return omClientResponse;
  }

  /**
   * Get a response builder for the request.
   * @return OMResponse builder
   */
  abstract OMResponse.Builder getResBuilder();

  /**
   * Get the OM client response on a success case with lock.
   */
  abstract OMClientResponse onSuccess(
      OMResponse.Builder omResponse, OmBucketInfo omBucketInfo,
      boolean operationResult);

  /**
   * Get the OM client response on a failure case with lock.
   */
  OMClientResponse onFailure(OMResponse.Builder omResponse,
                             Exception exception) {
    return new OmBucketNotificationResponse(
        createErrorOMResponse(omResponse, exception));
  }

  /**
   * Completion hook for final processing before return without lock.
   * Usually used for logging without a lock and metric update.
   */
  abstract void onComplete(boolean operationResult, Exception exception,
                           OMMetrics omMetrics, AuditLogger auditLogger,
                           Map<String, String> auditMap);

  private Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    return auditMap;
  }
}
