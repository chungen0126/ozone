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

package org.apache.hadoop.ozone.shell;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.S3NotificationInfo;
import picocli.CommandLine;

/**
 * Defines command-line option for specifying one or more Notifications.
 */
public class NotificationOptions implements CommandLine.ITypeConverter<S3NotificationInfo> {

  @CommandLine.Option(names = {"--notifications", "-n"}, split = ",",
      required = true,
      converter = NotificationOptions.class,
      description = "List of notifications to be sent. ")
  private S3NotificationInfo[] notifications;

  private List<S3NotificationInfo> getNotifications() {
    return ImmutableList.copyOf(notifications);
  }

  public void setOn(OzoneBucket bucket, PrintWriter out) throws IOException {
    if (bucket.setS3Notification(getNotifications())) {
      out.println("Notifications set successfully.");
    } else {
      out.println("Failed to set notifications.");
    }
  }

  @Override
  public S3NotificationInfo convert(String arg) {
    return S3NotificationInfo.parseAcl(arg);
  }
}
