/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.jmh;

import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main class that executes a set of Ozone benchmarks.
 */
@Command(name = "ozone benchmark",
    description = "Tool for running ozone benchmarks",
    mixinStandardHelpOptions = true)
public final class BenchmarkRunner {
  private BenchmarkRunner() {
  }
  @Option(names = "-t", defaultValue = "4",
      description = "Number of threads to use for the benchmark.\n"
          + "This option can be overridden by threads mentioned in benchmark.")
  private static int numThreads;
  public static void main(String[] args) throws RunnerException {
    CommandLine commandLine = new CommandLine(new BenchmarkRunner());
    commandLine.parse(args);
    if (commandLine.isUsageHelpRequested()) {
      commandLine.usage(System.out);
      return;
    }

    OptionsBuilder optionsBuilder = new OptionsBuilder();

    optionsBuilder.include("BenchmarkKeyValueContainerCheck");
    optionsBuilder.warmupIterations(2)
        .measurementIterations(20)
        .addProfiler(StackProfiler.class)
        .addProfiler(GCProfiler.class)
        .shouldDoGC(true)
        .forks(1)
        .threads(numThreads);

    new Runner(optionsBuilder.build()).run();
  }
}
