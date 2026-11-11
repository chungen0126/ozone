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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link BlockOutputStream#abort()}.
 */
class TestBlockOutputStreamAbort {

  private static final int BUFFER_SIZE = 1024;

  private final ExecutorService responseExecutor = Executors.newSingleThreadExecutor();
  private final Pipeline pipeline = MockPipeline.createRatisPipeline();
  private final HoldingXceiverClient xceiverClient = new HoldingXceiverClient(pipeline);
  private final XceiverClientManager xceiverClientManager = mock(XceiverClientManager.class);

  @AfterEach
  void shutdown() {
    responseExecutor.shutdownNow();
  }

  @Test
  void abortDoesNotSendBufferedDataOrPutBlock() throws Exception {
    BlockOutputStream out = createBlockOutputStream(false);
    // The first buffer is sent by WriteChunk, the second one is only half filled.
    out.write(new byte[BUFFER_SIZE + BUFFER_SIZE / 2]);
    xceiverClient.completeWriteChunks(true);

    out.abort();

    assertEquals(1, xceiverClient.getCommandCount(Type.WriteChunk));
    assertEquals(0, xceiverClient.getCommandCount(Type.PutBlock));
    assertEquals(0, xceiverClient.getWatchForCommitCount());
    assertTrue(out.isClosed());
    verify(xceiverClientManager).releaseClient(xceiverClient, false);
  }

  @Test
  void abortDoesNotWaitForCommit() throws Exception {
    BlockOutputStream out = createBlockOutputStream(false);
    xceiverClient.holdWatchForCommit();
    // Filling the flush period sends a PutBlock, whose commit is watched but never acknowledged here.
    out.write(new byte[4 * BUFFER_SIZE]);
    xceiverClient.completeWriteChunks(true);
    assertEquals(1, xceiverClient.getCommandCount(Type.PutBlock));

    startAbort(out, false).get(10, TimeUnit.SECONDS);

    assertTrue(out.isClosed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void abortWaitsForSentWriteChunk(boolean writeChunkSucceeds) throws Exception {
    BlockOutputStream out = createBlockOutputStream(false);
    out.write(new byte[BUFFER_SIZE]);

    FutureTask<Boolean> abort = startAbort(out, false);
    // The sent WriteChunk may still read the buffer, so abort must not return before it completes.
    assertThrows(TimeoutException.class, () -> abort.get(100, TimeUnit.MILLISECONDS));

    xceiverClient.completeWriteChunks(writeChunkSucceeds);
    abort.get(10, TimeUnit.SECONDS);
    assertTrue(out.isClosed());
  }

  @Test
  void interruptedAbortStillWaitsForSentWriteChunk() throws Exception {
    BlockOutputStream out = createBlockOutputStream(false);
    out.write(new byte[BUFFER_SIZE]);

    FutureTask<Boolean> abort = startAbort(out, true);
    assertThrows(TimeoutException.class, () -> abort.get(100, TimeUnit.MILLISECONDS));

    xceiverClient.completeWriteChunks(true);
    assertTrue(abort.get(10, TimeUnit.SECONDS), "abort should keep the interrupt status");
    assertTrue(out.isClosed());
  }

  @Test
  void abortWaitsForWriteChunkSentByConcurrentFlush() throws Exception {
    RatisBlockOutputStream out = createBlockOutputStream(true);
    out.write(new byte[BUFFER_SIZE / 2]);
    // With PutBlock piggybacking, the flush sends the half filled buffer in a WriteChunk carrying the PutBlock.
    CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> {
      try {
        out.hflush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    GenericTestUtils.waitFor(() -> xceiverClient.getCommandCount(Type.WriteChunk) == 1, 10, 10000);
    assertEquals(0, xceiverClient.getCommandCount(Type.PutBlock));

    FutureTask<Boolean> abort = startAbort(out, false);
    assertThrows(TimeoutException.class, () -> abort.get(100, TimeUnit.MILLISECONDS));

    xceiverClient.completeWriteChunks(true);
    abort.get(10, TimeUnit.SECONDS);
    assertTrue(out.isClosed());
    // The flush fails if abort releases the client first, so only wait for it to finish.
    flush.handle((result, e) -> null).get(10, TimeUnit.SECONDS);
  }

  @Test
  void abortIsNotSupportedForEcBlockStream() throws IOException {
    Pipeline ecPipeline = MockPipeline.createEcPipeline();
    when(xceiverClientManager.acquireClient(any())).thenReturn(new HoldingXceiverClient(ecPipeline));
    OzoneClientConfig config = new OzoneClientConfig();

    try (ECBlockOutputStream out = new ECBlockOutputStream(new BlockID(1L, 1L), xceiverClientManager, ecPipeline,
        BufferPool.empty(), config, null, ContainerClientMetrics.acquire(),
        StreamBufferArgs.getDefaultStreamBufferArgs(ecPipeline.getReplicationConfig(), config),
        () -> responseExecutor, true)) {
      // Its WriteChunk requests are not tracked, so abort could release buffers they still read.
      assertThrows(UnsupportedOperationException.class, out::abort);
    }
  }

  private RatisBlockOutputStream createBlockOutputStream(boolean putBlockPiggybacking) throws IOException {
    when(xceiverClientManager.acquireClient(any())).thenReturn(xceiverClient);

    OzoneClientConfig config = new OzoneClientConfig();
    config.setStreamBufferSize(BUFFER_SIZE);
    config.setStreamBufferFlushSize(4 * BUFFER_SIZE);
    config.setStreamBufferMaxSize(8 * BUFFER_SIZE);
    config.setChecksumType(ChecksumType.NONE);
    config.setEnablePutblockPiggybacking(putBlockPiggybacking);
    StreamBufferArgs streamBufferArgs =
        StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(), config);

    return new RatisBlockOutputStream(new BlockID(1L, 1L), -1, xceiverClientManager, pipeline,
        new BufferPool(BUFFER_SIZE, 8), config, null, ContainerClientMetrics.acquire(), streamBufferArgs,
        () -> responseExecutor);
  }

  /**
   * Runs abort in a new thread, interrupted beforehand if requested.
   * The task returns whether the thread is still interrupted after abort.
   */
  private static FutureTask<Boolean> startAbort(BlockOutputStream out, boolean interrupted) {
    FutureTask<Boolean> abort = new FutureTask<>(() -> {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      out.abort();
      return Thread.currentThread().isInterrupted();
    });
    new Thread(abort).start();
    return abort;
  }

  /**
   * Holds the WriteChunk replies until {@link #completeWriteChunks(boolean)}, and the watchForCommit replies forever
   * after {@link #holdWatchForCommit()}. Replies to other commands at once.
   */
  private static class HoldingXceiverClient extends XceiverClientSpi {

    private final Pipeline pipeline;
    private final AtomicInteger logIndex = new AtomicInteger();
    private final List<ContainerCommandRequestProto> commands = new CopyOnWriteArrayList<>();
    private final List<Consumer<Boolean>> heldWriteChunks = new CopyOnWriteArrayList<>();
    private final AtomicInteger watchForCommitCount = new AtomicInteger();
    private volatile boolean holdWatchForCommit;

    HoldingXceiverClient(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    void completeWriteChunks(boolean success) {
      heldWriteChunks.forEach(reply -> reply.accept(success));
    }

    void holdWatchForCommit() {
      holdWatchForCommit = true;
    }

    long getCommandCount(Type type) {
      return commands.stream().filter(c -> c.getCmdType() == type).count();
    }

    int getWatchForCommitCount() {
      return watchForCommitCount.get();
    }

    @Override
    public void connect() {
    }

    @Override
    public void close() {
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {
      commands.add(request);
      final CompletableFuture<ContainerCommandResponseProto> reply = new CompletableFuture<>();
      if (request.getCmdType() == Type.WriteChunk) {
        heldWriteChunks.add(success -> {
          if (success) {
            reply.complete(successResponse(request));
          } else {
            reply.completeExceptionally(new IOException("WriteChunk failed"));
          }
        });
      } else {
        reply.complete(successResponse(request));
      }
      XceiverClientReply result = new XceiverClientReply(reply);
      result.setLogIndex(logIndex.incrementAndGet());
      return result;
    }

    private static ContainerCommandResponseProto successResponse(ContainerCommandRequestProto request) {
      ContainerCommandResponseProto.Builder response = ContainerCommandResponseProto.newBuilder()
          .setCmdType(request.getCmdType())
          .setResult(Result.SUCCESS);
      if (request.getCmdType() == Type.PutBlock) {
        response.setPutBlock(PutBlockResponseProto.newBuilder()
            .setCommittedBlockLength(committedBlockLength(request.getPutBlock().getBlockData())));
      } else if (request.getCmdType() == Type.WriteChunk && request.getWriteChunk().hasBlock()) {
        // A piggybacked PutBlock
        response.setWriteChunk(WriteChunkResponseProto.newBuilder()
            .setCommittedBlockLength(committedBlockLength(request.getWriteChunk().getBlock().getBlockData())));
      }
      return response.build();
    }

    private static GetCommittedBlockLengthResponseProto committedBlockLength(BlockData blockData) {
      return GetCommittedBlockLengthResponseProto.newBuilder()
          .setBlockID(blockData.getBlockID())
          .setBlockLength(blockData.getSize())
          .build();
    }

    @Override
    public ReplicationType getPipelineType() {
      return ReplicationType.RATIS;
    }

    @Override
    public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
      watchForCommitCount.incrementAndGet();
      if (holdWatchForCommit) {
        return new CompletableFuture<>();
      }
      XceiverClientReply reply = new XceiverClientReply(CompletableFuture.completedFuture(
          ContainerCommandResponseProto.newBuilder().setCmdType(Type.WriteChunk).setResult(Result.SUCCESS).build()));
      reply.setLogIndex(index);
      return CompletableFuture.completedFuture(reply);
    }

    @Override
    public long getReplicatedMinCommitIndex() {
      return 0;
    }

    @Override
    public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
        ContainerCommandRequestProto request) {
      return null;
    }
  }
}
