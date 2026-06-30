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

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Benchmark tool for FSDataInputStream#readVectored performance.
 */
@Command(name = "dfsvr",
    aliases = "dfs-vectored-read-benchmark",
    description = "Benchmark FSDataInputStream#readVectored on any DFS compatible file system.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class HadoopFsVectoredReadValidator extends HadoopBaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFsVectoredReadValidator.class);

  @Option(names = {"--range-size"},
      description = "Size of each individual range read.",
      defaultValue = "65536")
  private int rangeSize;

  @Option(names = {"--range-count"},
      description = "Number of ranges to read in a single vectored read call.",
      defaultValue = "10")
  private int rangeCount;

  @Option(names = {"--range-gap"},
      description = "Gap in bytes between consecutive ranges to read.",
      defaultValue = "1048576")
  private int rangeGap;

  @Option(names = {"--direct-buffer"},
      description = "If true, allocates direct ByteBuffers, otherwise heap ByteBuffers.",
      defaultValue = "false")
  private boolean directBuffer;

  private Timer timer;

  @Override
  public Void call() throws Exception {
    super.init();
    timer = getMetrics().timer("vectored-read");

    runTests(this::benchmarkVectoredRead);
    return null;
  }

  private void benchmarkVectoredRead(long counter) throws Exception {
    Path file = new Path(getRootPath() + "/" + generateObjectName(counter));
    FileSystem fileSystem = getFileSystem();

    long fileLength = fileSystem.getFileStatus(file).getLen();
    long totalRequired = (long) rangeSize * rangeCount + (long) rangeGap * (rangeCount - 1);
    if (totalRequired > fileLength) {
      throw new IllegalArgumentException(String.format(
          "File %s size (%d) is too small for the configured ranges. Required: %d",
          file.getName(), fileLength, totalRequired));
    }

    List<FileRange> ranges = new ArrayList<>();
    long offset = 0;
    for (int i = 0; i < rangeCount; i++) {
      ranges.add(FileRange.createFileRange(offset, rangeSize));
      offset += rangeSize + rangeGap;
    }

    IntFunction<ByteBuffer> allocator = directBuffer ? ByteBuffer::allocateDirect : ByteBuffer::allocate;

    timer.time(() -> {
      try (FSDataInputStream input = fileSystem.open(file)) {
        input.readVectored(ranges, allocator);

        CompletableFuture<?>[] futures = ranges.stream()
            .map(FileRange::getData)
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).get(120, TimeUnit.SECONDS);

        for (FileRange range : ranges) {
          ByteBuffer buffer =  range.getData().get();

          if (buffer == null) {
            throw new IllegalStateException("buffer is null");
          }

          if (buffer.remaining() != range.getLength()) {
            throw new IllegalStateException(
                String.format("Invalid buffer size！ Expected: %d, Actual: %d.",
                    range.getLength(), buffer.remaining()));
          }

        }
      } catch (Exception e) {
        LOG.error("Failed executing vectored read on file {}", file, e);
        throw new IOException(e);
      }
      return null;
    });
  }
}
