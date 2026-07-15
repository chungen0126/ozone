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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.ozone.OzoneFSInputStream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Micro-benchmark for {@link OzoneFSInputStream#readVectored}.
 * Compares the performance of Vectored Read with Positioned Read Enabled vs. Disabled.
 * Reuses the first-party {@link DummyBlockInputStream} and {@link DummyChunkInputStream} for mocking.
 */
public class OzoneFSInputStreamReadVectoredBenchmark {

  private static final int FILE_SIZE = 100 * 1024 * 1024; // 100 MB
  private static final int BLOCK_SIZE = 20 * 1024 * 1024; // 20 MB (5 blocks total)
  private static final int CHUNK_SIZE = 4 * 1024 * 1024;  // 4 MB (5 chunks per block)

  private static final int WARMUP_ITERATIONS = 5;
  private static final int MEASURE_ITERATIONS = 10;

  // Global mock data to verify correctness
  private static final byte[] MOCK_FILE_DATA = new byte[FILE_SIZE];

  static {
    new Random(42).nextBytes(MOCK_FILE_DATA);
  }

  /**
   * Helper to create a mock OzoneFSInputStream using DummyBlockInputStream.
   */
  private OzoneFSInputStream createMockStream(boolean isPositionedReadable) throws IOException {
    List<BlockExtendedInputStream> blockStreams = new ArrayList<>();
    int numBlocks = (FILE_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE;
    XceiverClientFactory mockClientFactory = org.mockito.Mockito.mock(XceiverClientFactory.class);

    for (int b = 0; b < numBlocks; b++) {
      long blockGlobalOffset = (long) b * BLOCK_SIZE;
      long currentBlockSize = Math.min(BLOCK_SIZE, FILE_SIZE - blockGlobalOffset);
      BlockID blockId = new BlockID(1, b + 1);

      org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData dummyChecksum =
          org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData.newBuilder()
              .setType(org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType.NONE)
              .setBytesPerChecksum(4 * 1024 * 1024)
              .build();

      List<ChunkInfo> chunks = new ArrayList<>();
      Map<String, byte[]> chunkDataMap = new HashMap<>();
      int numChunks = (int) ((currentBlockSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
      for (int c = 0; c < numChunks; c++) {
        long chunkOffset = (long) c * CHUNK_SIZE;
        long currentChunkSize = Math.min(CHUNK_SIZE, currentBlockSize - chunkOffset);
        String chunkName = "block-" + (b + 1) + "-chunk-" + (c + 1);
        ChunkInfo chunk = ChunkInfo.newBuilder()
            .setChunkName(chunkName)
            .setOffset(chunkOffset)
            .setLen(currentChunkSize)
            .setChecksumData(dummyChecksum)
            .build();
        chunks.add(chunk);

        byte[] chunkBytes = new byte[(int) (chunkOffset + currentChunkSize)];
        System.arraycopy(MOCK_FILE_DATA, (int) (blockGlobalOffset + chunkOffset),
            chunkBytes, (int) chunkOffset, (int) currentChunkSize);
        chunkDataMap.put(chunkName, chunkBytes);
      }

      // Instantiate DummyBlockInputStream directly
      DummyBlockInputStream blockStream = new DummyBlockInputStream(
          blockId,
          currentBlockSize,
          null,
          null,
          mockClientFactory,
          null,
          chunks,
          chunkDataMap,
          new OzoneClientConfig()
      );
      blockStreams.add(blockStream);
    }

    KeyInputStream keyStream = new KeyInputStream("mock-key", blockStreams);
    return new OzoneFSInputStream(keyStream, null, isPositionedReadable);
  }

  @Test
  public void runBenchmark() throws Exception {
    System.out.println("======================================================================");
    System.out.println(" Starting OzoneFSInputStream#readVectored Micro-Benchmark");
    System.out.println("======================================================================");
    System.out.printf("File Size   : %d MB%n", FILE_SIZE / (1024 * 1024));
    System.out.printf("Block Size  : %d MB%n", BLOCK_SIZE / (1024 * 1024));
    System.out.printf("Chunk Size  : %d MB%n", CHUNK_SIZE / (1024 * 1024));
    System.out.println("======================================================================");

    // 1. Consecutive range pattern (20 ranges, 1 MB each)
    List<FileRange> consecutiveRanges = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      consecutiveRanges.add(FileRange.createFileRange((long) i * 1024 * 1024, 1024 * 1024));
    }

    // 2. Random range pattern (20 ranges, 1 MB each, spread randomly)
    List<FileRange> randomRanges = new ArrayList<>();
    Random rand = new Random(1337);
    for (int i = 0; i < 20; i++) {
      long offset = (long) rand.nextInt((FILE_SIZE - 1024 * 1024) / (1024 * 1024)) * 1024 * 1024;
      randomRanges.add(FileRange.createFileRange(offset, 1024 * 1024));
    }

    for (int i = 0; i < randomRanges.size() - 1; i++) {
      if (randomRanges.get(i).getOffset() + randomRanges.get(i).getLength() > randomRanges.get(i + 1).getOffset()) {
        randomRanges.remove(i + 1);
        i--;
      }
    }

    // 3. Sparse range pattern (5 ranges, 128 KB each, widely separated)
    List<FileRange> sparseRanges = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sparseRanges.add(FileRange.createFileRange(i * BLOCK_SIZE + 512 * 1024, 128 * 1024));
    }

    // Benchmark Consecutive
    benchmarkPattern("Consecutive Ranges", consecutiveRanges);

    // Benchmark Random
    benchmarkPattern("Random Ranges", randomRanges);

    // Benchmark Sparse
    benchmarkPattern("Sparse Ranges", sparseRanges);
  }

  private void benchmarkPattern(String patternName, List<FileRange> ranges) throws Exception {
    System.out.println("%n--- Pattern: " + patternName + " ---");
    System.out.println("Number of ranges: " + ranges.size());
    long totalBytes = ranges.stream().mapToLong(FileRange::getLength).sum();
    System.out.printf("Total read size : %.2f MB%n", (double) totalBytes / (1024 * 1024));

    // Warm-up
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      OzoneFSInputStream stream = createMockStream(true);
      cleanUpRanges(ranges);
      executeVectoredRead(stream, ranges);
      stream.close();

      stream = createMockStream(false);
      cleanUpRanges(ranges);
      executeVectoredRead(stream, ranges);
      stream.close();
    }

    // Measurement - Positioned Read Enabled (true)
    long[] enabledTimes = new long[MEASURE_ITERATIONS];
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
      OzoneFSInputStream stream = createMockStream(true);
      cleanUpRanges(ranges);
      long start = System.nanoTime();
      executeVectoredRead(stream, ranges);
      enabledTimes[i] = System.nanoTime() - start;
      stream.close();
      // Verify data correctness
      for (FileRange range : ranges) {
        ByteBuffer buf = range.getData().get();
        byte[] expected = new byte[range.getLength()];
        System.arraycopy(MOCK_FILE_DATA, (int) range.getOffset(), expected, 0, range.getLength());
        byte[] actual = new byte[buf.remaining()];
        buf.get(actual);
        Assertions.assertArrayEquals(expected, actual, "Data mismatch in read range!");
      }
    }

    // Measurement - Positioned Read Disabled (false)
    long[] disabledTimes = new long[MEASURE_ITERATIONS];
    for (int i = 0; i < MEASURE_ITERATIONS; i++) {
      OzoneFSInputStream stream = createMockStream(false);
      cleanUpRanges(ranges);
      long start = System.nanoTime();
      executeVectoredRead(stream, ranges);
      disabledTimes[i] = System.nanoTime() - start;
      stream.close();
      // Verify data correctness
      for (FileRange range : ranges) {
        ByteBuffer buf = range.getData().get();
        byte[] expected = new byte[range.getLength()];
        System.arraycopy(MOCK_FILE_DATA, (int) range.getOffset(), expected, 0, range.getLength());
        byte[] actual = new byte[buf.remaining()];
        buf.get(actual);
        Assertions.assertArrayEquals(expected, actual, "Data mismatch in read range!");
      }
    }

    // Report Results
    reportResults("Vectored Read (Positioned Enabled=true)", enabledTimes, totalBytes);
    reportResults("Vectored Read (Positioned Enabled=false)", disabledTimes, totalBytes);
  }

  private void executeVectoredRead(OzoneFSInputStream stream, List<FileRange> ranges) throws Exception {

    stream.readVectored(ranges, ByteBuffer::allocate);

    CompletableFuture<?>[] futures = ranges.stream()
        .map(FileRange::getData)
        .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
  }

  private void reportResults(String label, long[] nanoTimes, long totalBytes) {
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long t : nanoTimes) {
      sum += t;
      if (t < min) {
        min = t;
      }
      if (t > max) {
        max = t;
      }
    }
    double meanMs = (double) sum / nanoTimes.length / 1_000_000.0;
    double minMs = (double) min / 1_000_000.0;
    double maxMs = (double) max / 1_000_000.0;

    double mb = (double) totalBytes / (1024.0 * 1024.0);
    double throughputMBs = mb / (meanMs / 1000.0);

    System.out.printf("  %-40s : Mean: %7.2f ms | Min: %7.2f ms | Max: %7.2f ms | Throughput: %7.2f MB/s%n",
        label, meanMs, minMs, maxMs, throughputMBs);
  }

  public static void main(String[] args) throws Exception {
    OzoneFSInputStreamReadVectoredBenchmark benchmark = new OzoneFSInputStreamReadVectoredBenchmark();
    benchmark.runBenchmark();
  }

  private void cleanUpRanges(List<FileRange> ranges) {
    for (FileRange range : ranges) {
      range.setData(null);
    }
  }
}
