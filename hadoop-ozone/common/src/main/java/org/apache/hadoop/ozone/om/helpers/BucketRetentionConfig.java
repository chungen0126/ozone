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

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Retention configuration for a bucket.
 */
@Immutable
public final class BucketRetentionConfig {
  private final String retentionMode;
  private final long retentionPeriodDays;
  private final long retentionPeriodYears;

  public BucketRetentionConfig(String retentionMode, long retentionPeriodDays, long retentionPeriodYears) {
    this.retentionMode = retentionMode;
    this.retentionPeriodDays = retentionPeriodDays;
    this.retentionPeriodYears = retentionPeriodYears;
  }

  public String getRetentionMode() {
    return retentionMode;
  }

  public long getRetentionPeriodDays() {
    return retentionPeriodDays;
  }

  public long getRetentionPeriodYears() {
    return retentionPeriodYears;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BucketRetentionConfig that = (BucketRetentionConfig) o;
    return retentionPeriodDays == that.retentionPeriodDays
        && retentionPeriodYears == that.retentionPeriodYears
        && Objects.equals(retentionMode, that.retentionMode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(retentionMode, retentionPeriodDays, retentionPeriodYears);
  }

  public OzoneManagerProtocolProtos.BucketRetentionConfig toProtobuf() {
    OzoneManagerProtocolProtos.BucketRetentionConfig.Builder builder =
        OzoneManagerProtocolProtos.BucketRetentionConfig.newBuilder();
    if (retentionMode != null) {
      builder.setRetentionMode(retentionMode);
    }
    builder.setRetentionPeriodDays(retentionPeriodDays);
    builder.setRetentionPeriodYears(retentionPeriodYears);
    return builder.build();
  }

  public static BucketRetentionConfig fromProtobuf(OzoneManagerProtocolProtos.BucketRetentionConfig proto) {
    return new BucketRetentionConfig(
        proto.hasRetentionMode() ? proto.getRetentionMode() : null,
        proto.hasRetentionPeriodDays() ? proto.getRetentionPeriodDays() : 0,
        proto.hasRetentionPeriodYears() ? proto.getRetentionPeriodYears() : 0);
  }
}
