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

package org.apache.hadoop.ozone.debug.segmentparser;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import picocli.CommandLine;

/**
 * Command line utility to parse and dump a SCM ratis segment file.
 */
@CommandLine.Command(
    name = "scm",
    description = "dump scm ratis segment file",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SCMRatisLogParser extends BaseLogParser implements Callable<Void> {

  @Override
  public Void call() throws Exception {
    System.out.println("Dumping SCM Ratis Log");

    parseRatisLogs(SCMRatisRequest::smProtoToString);
    return null;
  }
}
