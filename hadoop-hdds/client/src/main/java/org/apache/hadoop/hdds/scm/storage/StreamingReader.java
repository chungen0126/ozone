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

import static org.apache.ratis.thirdparty.io.grpc.Status.Code.CANCELLED;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadBlockResponseProto;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.StreamingReaderSpi;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a StreamObserver used to receive and buffer streaming GRPC reads.
 */
public class StreamingReader implements StreamingReaderSpi {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingReader.class);
  private static final AtomicInteger READER_ID = new AtomicInteger(0);

  private final StreamingReaderContext context;
  private final String name;

  /** Response queue: poll is blocking while offer is non-blocking. */
  private final BlockingQueue<ReadBlockResponseProto> responseQueue = new LinkedBlockingQueue<>();

  private final CompletableFuture<Void> future = new CompletableFuture<>();
  private final AtomicBoolean semaphoreReleased = new AtomicBoolean(false);
  private final AtomicReference<StreamingReadResponse> response = new AtomicReference<>();

  public StreamingReader(StreamingReaderContext context) {
    this.context = context;
    this.name = context.getStreamName() + "-reader" + READER_ID.getAndIncrement();
  }

  void checkError() throws IOException {
    if (future.isCompletedExceptionally()) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Streaming read failed", e);
      }
    }
  }

  ReadBlockResponseProto poll() throws IOException {
    final long startTime = System.nanoTime();
    final long readTimeoutNanos = context.getReadTimeoutNanos();
    final long pollTimeoutNanos = Math.min(readTimeoutNanos / 10, 100_000_000);

    while (true) {
      checkError();

      final ReadBlockResponseProto proto;
      try {
        proto = responseQueue.poll(pollTimeoutNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for response", e);
      }
      if (proto != null) {
        return proto;
      }

      // Check isDone only after confirming the queue is empty. If isDone() were
      // checked first, an item delivered by onNext() just before onCompleted()
      // fired would be silently dropped, causing data corruption.
      if (future.isDone()) {
        return null; // Stream ended, queue is empty
      }

      final long elapsedNanos = System.nanoTime() - startTime;
      if (elapsedNanos >= readTimeoutNanos) {
        setFailedAndThrow(new TimeoutIOException(
            "Timed out waiting for response after " + context.getReadTimeout()));
        return null;
      }
    }
  }

  ByteBuffer read(int length, boolean preRead) throws IOException {
    checkError();
    if (future.isDone()) {
      // Don't return null while items remain in the queue. onNext() may have delivered items just before
      // onCompleted() fired.
      return responseQueue.isEmpty() ? null : readFromQueue();
    }

    context.readBlock(length, preRead);

    while (true) {
      final ByteBuffer buf = readFromQueue();
      if (buf != null && buf.hasRemaining()) {
        return buf;
      }
    }
  }

  ByteBuffer readFromQueue() throws IOException {
    final ReadBlockResponseProto readBlock = poll();
    // The server always returns data starting from the last checksum boundary. Therefore if the reader position is
    // ahead of the position we received from the server, we need to adjust the buffer position accordingly.
    // If the reader position is behind
    final ByteString data = readBlock.getData();
    final ByteBuffer dataBuffer = data.asReadOnlyByteBuffer();
    final long blockOffset = readBlock.getOffset();
    final long pos = context.getPos();
    if (pos < blockOffset) {
      // This should not happen, and if it does, we have a bug.
      setFailedAndThrow(new IllegalStateException(
          this + ": out of order, position " + pos + " < block offset " + blockOffset));
    }
    final long offset = pos - blockOffset;
    if (offset > 0) {
      dataBuffer.position(Math.toIntExact(Math.min(offset, dataBuffer.limit())));
    }
    LOG.debug("{}: return response positon {}, length {} (block offset {}, length {})",
        name, pos, dataBuffer.remaining(), blockOffset, data.size());
    return dataBuffer;
  }

  private void releaseResources() {
    if (semaphoreReleased.compareAndSet(false, true)) {
      context.releaseStreamResources();
    }
  }

  @Override
  public void onNext(ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto) {
    final ReadBlockResponseProto readBlock = containerCommandResponseProto.getReadBlock();
    try {
      ByteBuffer data = readBlock.getData().asReadOnlyByteBuffer();
      if (context.isVerifyChecksum()) {
        ChecksumData checksumData = ChecksumData.getFromProtoBuf(readBlock.getChecksumData());
        Checksum.verifyChecksum(data, checksumData, 0);
      }
      offerToQueue(readBlock);
    } catch (Exception e) {
      final ByteString data = readBlock.getData();
      final long offset = readBlock.getOffset();
      final StreamingReadResponse r = getResponse();
      LOG.warn("Failed to process block {} response at offset={}, size={}: {}, {}",
          context.getBlockID().getContainerBlockID(),
          offset, data.size(), StringUtils.bytes2Hex(data.substring(0, 10).asReadOnlyByteBuffer()),
          readBlock.getChecksumData(), e);
      setFailed(e);
      r.getRequestObserver().onError(e);
      releaseResources();
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (throwable instanceof StatusRuntimeException) {
      if (((StatusRuntimeException) throwable).getStatus().getCode() == CANCELLED) {
        // This is expected when the client cancels the stream.
        setCompleted();
      }
    } else {
      setFailed(throwable);
    }
    releaseResources();
  }

  @Override
  public void onCompleted() {
    setCompleted();
    releaseResources();
  }

  StreamingReadResponse getResponse() {
    return response.get();
  }

  private <T extends Throwable> void setFailedAndThrow(T throwable) throws T {
    if (setFailed(throwable)) {
      throw throwable;
    }
  }

  private boolean setFailed(Throwable throwable) {
    final boolean completed = future.completeExceptionally(throwable);
    if (!completed) {
      LOG.warn("{}: Already completed, suppress ", this, throwable);
    }
    return completed;
  }

  private void setCompleted() {
    final boolean changed = future.complete(null);
    if (changed) {
      LOG.debug("{} setCompleted success", this);
    } else {
      try {
        future.get();
        LOG.debug("{} Failed to setCompleted: Already completed", this);
      } catch (InterruptedException e) {
        LOG.warn("{}: Interrupted setCompleted", this, e);
      } catch (ExecutionException e) {
        LOG.warn("{}: Failed to setCompleted: already completed exceptionally", this, e);
      }
    }

    releaseResources();
  }

  private void offerToQueue(ReadBlockResponseProto item) {
    if (LOG.isDebugEnabled()) {
      final ContainerProtos.ChecksumData checksumData = item.getChecksumData();
      LOG.debug("{}: enqueue response offset {}, length {}, numChecksums {}, bytesPerChecksum={}",
          name, item.getOffset(), item.getData().size(),
          checksumData.getChecksumsList().size(), checksumData.getBytesPerChecksum());
    }
    final boolean offered = responseQueue.offer(item);
    Preconditions.assertTrue(offered, () -> "Failed to offer " + item);
  }

  @Override
  public void setStreamingReadResponse(StreamingReadResponse streamingReadResponse) {
    final boolean set = response.compareAndSet(null, streamingReadResponse);
    Preconditions.assertTrue(set, () -> "Failed to set streamingReadResponse");
  }

  @Override
  public String toString() {
    return name;
  }
}
