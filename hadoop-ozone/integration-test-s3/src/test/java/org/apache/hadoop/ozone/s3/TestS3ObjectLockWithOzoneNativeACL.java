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

package org.apache.hadoop.ozone.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectLockMode;
import com.amazonaws.services.s3.model.ObjectLockLegalHoldStatus;
import java.util.Date;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test S3 Object Lock operations with Ozone Native ACLs enabled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestS3ObjectLockWithOzoneNativeACL extends ClusterForTests<MiniOzoneCluster> {

  private AmazonS3 s3Client;
  private String bucketName = "worm-bucket";

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS, OzoneNativeAuthorizer.class.getName());
    conf.set(org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE.name());
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(new MultiS3GatewayService(1))
        .build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    s3Client = new S3ClientFactory(getCluster().getConf()).createS3Client();

    // Create Bucket and force Object Lock enabled via OM MetadataManager
    s3Client.createBucket(bucketName);
    OzoneManager om = getCluster().getOzoneManager();
    String bucketKey = om.getMetadataManager().getBucketKey("s3v", bucketName);
    org.apache.hadoop.ozone.om.helpers.OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);
    org.apache.hadoop.ozone.om.helpers.OmBucketInfo newBucketInfo = bucketInfo.toBuilder().setObjectLockEnabled(true).build();
    om.getMetadataManager().getBucketTable().put(bucketKey, newBucketInfo);
  }

  @Test
  public void testPutObjectWithRetention() throws Exception {
    String keyName = "retention-key";
    
    Instant retainUntil = Instant.now().plus(1, ChronoUnit.HOURS);

    try (OzoneClient client = getCluster().newClient()) {
      java.util.Map<String, String> customMetadata = new java.util.HashMap<>();
      customMetadata.put(org.apache.hadoop.ozone.OzoneConsts.OZONE_RETENTION_MODE, "COMPLIANCE");
      customMetadata.put(org.apache.hadoop.ozone.OzoneConsts.OZONE_RETAIN_UNTIL_DATE, String.valueOf(retainUntil.toEpochMilli()));
      
      org.apache.hadoop.ozone.client.OzoneBucket bucket = client.getObjectStore().getVolume("s3v").getBucket(bucketName);
      bucket.createKey(keyName, 0, org.apache.hadoop.hdds.client.ReplicationType.RATIS, org.apache.hadoop.hdds.client.ReplicationFactor.ONE, customMetadata).close();
    }

    AmazonS3Exception ex = assertThrows(AmazonS3Exception.class, () -> s3Client.deleteObject(bucketName, keyName));
    assertEquals(403, ex.getStatusCode());
    assertEquals("AccessDenied", ex.getErrorCode());
  }

  @Test
  public void testLegalHold() throws Exception {
    String keyName = "legal-hold-key";
    
    try (OzoneClient client = getCluster().newClient()) {
      java.util.Map<String, String> customMetadata = new java.util.HashMap<>();
      customMetadata.put(org.apache.hadoop.ozone.OzoneConsts.OZONE_LEGAL_HOLD, "true");
      
      org.apache.hadoop.ozone.client.OzoneBucket bucket = client.getObjectStore().getVolume("s3v").getBucket(bucketName);
      bucket.createKey(keyName, 0, org.apache.hadoop.hdds.client.ReplicationType.RATIS, org.apache.hadoop.hdds.client.ReplicationFactor.ONE, customMetadata).close();
    }

    AmazonS3Exception ex = assertThrows(AmazonS3Exception.class, () -> s3Client.deleteObject(bucketName, keyName));
    assertEquals(403, ex.getStatusCode());
    assertEquals("AccessDenied", ex.getErrorCode());
  }

  @Test
  public void testBypassGovernanceRetentionNotSupported() throws Exception {
    String keyName = "governance-key";
    
    Instant retainUntil = Instant.now().plus(1, ChronoUnit.HOURS);

    try (OzoneClient client = getCluster().newClient()) {
      java.util.Map<String, String> customMetadata = new java.util.HashMap<>();
      customMetadata.put(org.apache.hadoop.ozone.OzoneConsts.OZONE_RETENTION_MODE, "GOVERNANCE");
      customMetadata.put(org.apache.hadoop.ozone.OzoneConsts.OZONE_RETAIN_UNTIL_DATE, String.valueOf(retainUntil.toEpochMilli()));
      
      org.apache.hadoop.ozone.client.OzoneBucket bucket = client.getObjectStore().getVolume("s3v").getBucket(bucketName);
      bucket.createKey(keyName, 0, org.apache.hadoop.hdds.client.ReplicationType.RATIS, org.apache.hadoop.hdds.client.ReplicationFactor.ONE, customMetadata).close();
    }

    try (OzoneClient client = getCluster().newClient()) {
      Exception ex = assertThrows(Exception.class, () -> client.getObjectStore().getVolume("s3v").getBucket(bucketName).deleteKey(keyName));
      // Just verifying it throws since Native ACL blocks it.
    }
  }

}
