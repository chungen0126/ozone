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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;

/**
 * Class which defines OzoneManager Ratis Server config.
 */
@ConfigGroup(prefix = OMConfigKeys.OZONE_OM_HA_PREFIX + "."
    + RaftServerConfigKeys.PREFIX)
public class OzoneManagerRatisServerConfig {
  /** @see RaftServerConfigKeys.Log.Appender#WAIT_TIME_MIN_KEY */
  @Config(key = "log.appender.wait-time.min",
      defaultValue = "0ms",
      type = ConfigType.TIME,
      tags = {OZONE, OM, RATIS, PERFORMANCE},
      description = "Minimum wait time between two appendEntries calls."
  )
  private long logAppenderWaitTimeMin;

  @Config(key = "retrycache.expirytime",
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {OZONE, OM, RATIS},
      description = "The timeout duration of the retry cache."
  )
  private long retryCacheTimeout = Duration.ofSeconds(300).toMillis();

  public long getLogAppenderWaitTimeMin() {
    return logAppenderWaitTimeMin;
  }

  public void setLogAppenderWaitTimeMin(long logAppenderWaitTimeMin) {
    this.logAppenderWaitTimeMin = logAppenderWaitTimeMin;
  }

  public long getRetryCacheTimeout() {
    return retryCacheTimeout;
  }

  public void setRetryCacheTimeout(Duration duration) {
    this.retryCacheTimeout = duration.toMillis();
  }
}
