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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamingReadResponse;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link java.io.InputStream} called from KeyInputStream to read a block from the
 * container.
 */
public class StreamBlockInputStream extends BlockExtendedInputStream
    implements StreamingReaderContext {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBlockInputStream.class);
  private static final int EOF = -1;
  private static final String STREAM_CLOSE_REASON = "StreamBlockInputStream closed";
  private static final AtomicInteger STREAM_ID = new AtomicInteger(0);

  private final String name = "stream" + STREAM_ID.getAndIncrement();
  private final BlockID blockID;
  private final long blockLength;
  private final int responseDataSize;
  private final long preReadSize;
  private final Duration readTimeout;
  private final long readTimeoutNanos;
  private final AtomicReference<Pipeline> pipelineRef = new AtomicReference<>();
  private final AtomicReference<Token<OzoneBlockTokenIdentifier>> tokenRef = new AtomicReference<>();
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientGrpc xceiverClient;

  private ByteBuffer buffer;
  private long position = 0;
  private long requestedLength = 0;
  private StreamingReader streamingReader;

  private final boolean verifyChecksum;
  private final Function<BlockID, BlockLocationInfo> refreshFunction;
  private final RetryPolicy retryPolicy;
  private int retries = 0;

  public StreamBlockInputStream(
      BlockID blockID, long length, Pipeline pipeline,
      Token<OzoneBlockTokenIdentifier> token,
      XceiverClientFactory xceiverClientFactory,
      Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig config) throws IOException {
    this.blockID = blockID;
    this.blockLength = length;
    pipelineRef.set(setPipeline(pipeline));
    tokenRef.set(token);
    this.xceiverClientFactory = xceiverClientFactory;
    this.verifyChecksum = config.isChecksumVerify();
    this.retryPolicy = getReadRetryPolicy(config);
    this.refreshFunction = refreshFunction;
    this.preReadSize = config.getStreamReadPreReadSize();
    this.responseDataSize = config.getStreamReadResponseDataSize();
    this.readTimeout = config.getStreamReadTimeout();
    this.readTimeoutNanos = readTimeout.toNanos();
  }

  @Override
  public BlockID getBlockID() {
    return blockID;
  }

  @Override
  public long getLength() {
    return blockLength;
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized int read() throws IOException {
    checkOpen();
    if (!dataAvailableToRead(1, true)) {
      return EOF;
    }
    int value = buffer.get();
    advancePosition(1);
    return value;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    ByteBuffer tmpBuffer = ByteBuffer.wrap(b, off, len);
    return read(tmpBuffer);
  }

  @Override
  public synchronized int read(ByteBuffer targetBuf) throws IOException {
    return readFully(targetBuf, true);
  }

  synchronized int readFully(ByteBuffer targetBuf, boolean preRead) throws IOException {
    checkOpen();
    int read = 0;
    while (targetBuf.hasRemaining()) {
      if (!dataAvailableToRead(targetBuf.remaining(), preRead)) {
        break;
      }
      int toCopy = Math.min(buffer.remaining(), targetBuf.remaining());
      ByteBuffer tmpBuf = buffer.duplicate();
      tmpBuf.limit(tmpBuf.position() + toCopy);
      targetBuf.put(tmpBuf);
      buffer.position(tmpBuf.position());
      advancePosition(toCopy);
      read += toCopy;
    }
    return read > 0 ? read : EOF;
  }

  private synchronized boolean dataAvailableToRead(int length, boolean preRead) throws IOException {
    if (position >= blockLength) {
      return false;
    }
    initialize();

    if (bufferHasRemaining()) {
      return true;
    }
    buffer = streamingReader.read(length, preRead);
    return bufferHasRemaining();
  }

  private synchronized void advancePosition(long delta) {
    position += delta;
    if (position >= blockLength && streamingReader != null) {
      closeStream();
    }
  }

  private synchronized boolean bufferHasRemaining() {
    return buffer != null && buffer.hasRemaining();
  }

  @Override
  protected int readWithStrategy(ByteReaderStrategy strategy) throws IOException {
    throw new NotImplementedException("readWithStrategy is not implemented.");
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    if (pos < 0) {
      throw new IOException("Cannot seek to negative offset");
    }
    if (pos > blockLength) {
      throw new EOFException("Failed to seek to position " + pos + " > block length = " + blockLength);
    }
    if (pos == position) {
      return;
    }
    LOG.debug("{}: seek {} -> {}", this, position, pos);
    closeStream();
    position = pos;
    requestedLength = pos;
  }

  @Override
  // The seekable interface indicates that seekToNewSource should seek to a new source of the data,
  // ie a different datanode. This is not supported for now.
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized void unbuffer() {
    releaseClient();
  }

  private synchronized void closeStream() {
    if (streamingReader == null) {
      buffer = null;
      return;
    }

    final StreamingReader reader = streamingReader;
    streamingReader = null;
    buffer = null;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing {}", reader);
    }

    reader.onCompleted();

    final StreamingReadResponse response = reader.getResponse();
    if (response != null) {
      final ClientCallStreamObserver<ContainerProtos.ContainerCommandRequestProto> requestObserver =
          response.getRequestObserver();
      try {
        requestObserver.onCompleted();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close gRPC request stream for {}", reader, e);
        try {
          requestObserver.cancel(STREAM_CLOSE_REASON, e);
        } catch (RuntimeException cancelEx) {
          LOG.warn("Failed to cancel gRPC request stream for {}", reader, cancelEx);
        }
      }
    }
  }

  protected synchronized void checkOpen() throws IOException {
    if (xceiverClientFactory == null) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED + " Block: " + blockID);
    }
  }

  protected synchronized void acquireClient() throws IOException {
    checkOpen();
    if (xceiverClient == null) {
      final Pipeline pipeline = pipelineRef.get();
      final XceiverClientSpi client;
      try {
        client = xceiverClientFactory.acquireClientForReadData(pipeline);
      } catch (IOException ioe) {
        LOG.warn("Failed to acquire client for pipeline {}, block {}", pipeline, blockID);
        throw ioe;
      }

      if (client == null) {
        throw new IOException("Failed to acquire client for " + pipeline);
      }
      if (!(client instanceof XceiverClientGrpc)) {
        throw new IOException("Unexpected client class: " + client.getClass().getName() + ", " + pipeline);
      }

      xceiverClient =  (XceiverClientGrpc) client;
    }
  }

  private synchronized void initialize() throws IOException {
    while (streamingReader == null) {
      try {
        acquireClient();
        final StreamingReader reader = new StreamingReader(this);
        xceiverClient.initStreamRead(blockID, reader);
        streamingReader = reader;
      } catch (IOException ioe) {
        handleExceptions(ioe);
      }
    }
  }

  @Override
  public synchronized void readBlock(int length, boolean preRead) throws IOException {
    final long required = position + length - requestedLength;
    final long preReadLength = preRead ? preReadSize : 0;
    final long readLength = required + preReadLength;

    if (readLength > 0) {
      LOG.debug("position {}, length {}, requested {}, diff {}, readLength {}, preReadSize={}",
          position, length, requestedLength, required, readLength, preReadLength);
      readBlockImpl(readLength);
      requestedLength += readLength;
    }
  }

  synchronized void readBlockImpl(long length) throws IOException {
    if (streamingReader == null) {
      throw new IOException("Uninitialized StreamingReader: " + blockID);
    }
    final StreamingReadResponse r = streamingReader.getResponse();
    if (r == null) {
      throw new IOException("Uninitialized StreamingReadResponse: " + blockID);
    }
    xceiverClient.streamRead(ContainerProtocolCalls.buildReadBlockCommandProto(
        blockID, requestedLength, length, responseDataSize, tokenRef.get(), pipelineRef.get()), r);
  }

  private void handleExceptions(IOException cause) throws IOException {
    if (cause instanceof StorageContainerException || isConnectivityIssue(cause)) {
      if (shouldRetryRead(cause, retryPolicy, retries++)) {
        releaseClient();
        refreshBlockInfo(cause);
        LOG.warn("Refreshing block data to read block {} due to {}", blockID, cause.getMessage());
      } else {
        throw cause;
      }
    } else {
      throw cause;
    }
  }

  protected synchronized void releaseClient() {
    if (xceiverClientFactory != null && xceiverClient != null) {
      closeStream();
      xceiverClientFactory.releaseClientForReadData(xceiverClient, false);
      xceiverClient = null;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    releaseClient();
    xceiverClientFactory = null;
  }

  private void refreshBlockInfo(IOException cause) throws IOException {
    refreshBlockInfo(cause, blockID, pipelineRef, tokenRef, refreshFunction);
  }

  @Override
  public synchronized void releaseStreamResources() {
    if (xceiverClient != null) {
      xceiverClient.completeStreamRead();
    }
  }

  @Override
  public String toString() {
    return name;
  }

  public long getPreReadSize() {
    return preReadSize;
  }

  public int getResponseDataSize() {
    return responseDataSize;
  }

  /** Visible for testing: returns the configured streaming read timeout. */
  @Override
  public Duration getReadTimeout() {
    return readTimeout;
  }

  @Override
  public String getStreamName() {
    return name;
  }

  @Override
  public boolean isVerifyChecksum() {
    return verifyChecksum;
  }

  @Override
  public long getReadTimeoutNanos() {
    return readTimeoutNanos;
  }
}
