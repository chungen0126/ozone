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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NotificationInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Event;

/**
 * S3 Notification Info.
 * This class encapsulates the information required for S3 notifications.
 */
public class S3NotificationInfo {
  private final String targetId;
  private final EventType eventType;

  public S3NotificationInfo(String targetId, EventType eventType) {
    this.targetId = targetId;
    this.eventType = eventType;
  }

  public static S3NotificationInfo fromProtobuf(
      NotificationInfo notificationInfo) {
    return new S3NotificationInfo(
        notificationInfo.getTargetId(),
        EventType.valueOf(notificationInfo.getEvent().name()));
  }

  public NotificationInfo toProtobuf() {
    return NotificationInfo.newBuilder()
        .setTargetId(targetId)
        .setEvent(S3Event.valueOf(eventType.name()))
        .build();
  }

  @Override
  public String toString() {
    return "S3NotificationInfo{" +
        "targetId='" + targetId + '\'' +
        ", eventType='" + eventType + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3NotificationInfo that = (S3NotificationInfo) o;
    return Objects.equals(targetId, that.getTargetId()) &&
        eventType == that.getEventType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetId, eventType);
  }

  public String getTargetId() {
    return targetId;
  }

  public EventType getEventType() {
    return eventType;
  }

  /**
   * Enum representing the type of S3 events for notifications.
   */
  public enum EventType {
    S3_TEST,
    S3_OBJECT_CREATE_PUT,
    S3_OBJECT_CREATE_COMPLETE_MULTIPART_UPLOAD,
    S3_OBJECT_REMOVE_DELETE,
    S3_OBJECT_TAGGING_PUT,
    S3_OBJECT_TAGGING_DELETE
  }

  public static S3NotificationInfo parseAcl(String notificationInfos)
      throws IllegalArgumentException {
    if (notificationInfos == null || notificationInfos.isEmpty()) {
      throw new IllegalArgumentException("Notification info cannot be null or empty");
    }
    String[] parts = notificationInfos.trim().split(":");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Notifications are not in expected format");
    }

    if ("S3".compareTo(parts[0].toUpperCase()) == 0) {
      return new S3NotificationInfo(parts[1], EventType.valueOf(parts[2].toUpperCase()));
    }

    throw new IllegalArgumentException("Notifications are not in expected format");
  }
}
