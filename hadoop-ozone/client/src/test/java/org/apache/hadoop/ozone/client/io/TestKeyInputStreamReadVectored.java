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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.ByteReaderStrategy;
import org.junit.jupiter.api.Test;

/**
 * Unit test class to verify and validate positioned read operations on KeyInputStream.
 */
public class TestKeyInputStreamReadVectored {

  private static class MockBlockInputStream extends BlockExtendedInputStream {
    private final BlockID blockID;
    private final long length;
    private final byte[] data;

    MockBlockInputStream(BlockID blockID, long length, byte[] data) {
      this.blockID = blockID;
      this.length = length;
      this.data = data;
    }

    @Override
    public BlockID getBlockID() {
      return blockID;
    }

    @Override
    public long getLength() {
      return length;
    }

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return 0;
    }

    @Override
    protected int readWithStrategy(ByteReaderStrategy strategy) throws IOException {
      return 0;
    }

    @Override
    public void seek(long pos) throws IOException {
    }

    @Override
    public boolean readFully(long position, ByteBuffer buffer) throws IOException {
      if (position < 0 || position >= length) {
        throw new EOFException("EOF");
      }
      int toRead = (int) Math.min(buffer.remaining(), length - position);
      buffer.put(data, (int) position, toRead);
      return true;
    }

    @Override
    public void unbuffer() {
    }
  }

  @Test
  public void testMultipleRangesInSingleBlock() throws IOException {
    byte[] blockData = new byte[100];
    for (int i = 0; i < 100; i++) {
      blockData[i] = (byte) i;
    }
    BlockID blockID = new BlockID(1, 1);
    MockBlockInputStream blockStream = new MockBlockInputStream(blockID, 100, blockData);
    
    KeyInputStream keyStream = new KeyInputStream("test-key", Arrays.asList(blockStream));

    byte[] target1 = new byte[20];
    ByteBuffer buf1 = ByteBuffer.wrap(target1);
    assertTrue(keyStream.readFully(10, buf1));
    assertEquals(0, buf1.remaining());
    byte[] expected1 = Arrays.copyOfRange(blockData, 10, 30);
    assertArrayEquals(expected1, target1);

    byte[] target2 = new byte[30];
    ByteBuffer buf2 = ByteBuffer.wrap(target2);
    assertTrue(keyStream.readFully(50, buf2));
    assertEquals(0, buf2.remaining());
    byte[] expected2 = Arrays.copyOfRange(blockData, 50, 80);
    assertArrayEquals(expected2, target2);
  }

  @Test
  public void testMultipleRangesAcrossMultipleBlocks() throws IOException {
    byte[] blockData1 = new byte[100];
    byte[] blockData2 = new byte[100];
    for (int i = 0; i < 100; i++) {
      blockData1[i] = (byte) i;
      blockData2[i] = (byte) (100 + i);
    }
    MockBlockInputStream block1 = new MockBlockInputStream(new BlockID(1, 1), 100, blockData1);
    MockBlockInputStream block2 = new MockBlockInputStream(new BlockID(1, 2), 100, blockData2);

    KeyInputStream keyStream = new KeyInputStream("test-key", Arrays.asList(block1, block2));

    byte[] target1 = new byte[20];
    ByteBuffer buf1 = ByteBuffer.wrap(target1);
    assertTrue(keyStream.readFully(50, buf1));
    assertEquals(0, buf1.remaining());
    byte[] expected1 = Arrays.copyOfRange(blockData1, 50, 70);
    assertArrayEquals(expected1, target1);

    byte[] target2 = new byte[30];
    ByteBuffer buf2 = ByteBuffer.wrap(target2);
    assertTrue(keyStream.readFully(120, buf2));
    assertEquals(0, buf2.remaining());
    byte[] expected2 = Arrays.copyOfRange(blockData2, 20, 50);
    assertArrayEquals(expected2, target2);
  }

  @Test
  public void testSingleRangeAcrossTwoBlocks() throws IOException {
    byte[] blockData1 = new byte[100];
    byte[] blockData2 = new byte[100];
    for (int i = 0; i < 100; i++) {
      blockData1[i] = (byte) i;
      blockData2[i] = (byte) (100 + i);
    }
    MockBlockInputStream block1 = new MockBlockInputStream(new BlockID(1, 1), 100, blockData1);
    MockBlockInputStream block2 = new MockBlockInputStream(new BlockID(1, 2), 100, blockData2);

    KeyInputStream keyStream = new KeyInputStream("test-key", Arrays.asList(block1, block2));

    byte[] target = new byte[50];
    ByteBuffer buf = ByteBuffer.wrap(target);
    assertTrue(keyStream.readFully(80, buf));
    assertEquals(0, buf.remaining());

    byte[] expected = new byte[50];
    System.arraycopy(blockData1, 80, expected, 0, 20);
    System.arraycopy(blockData2, 0, expected, 20, 30);
    assertArrayEquals(expected, target);
  }
}
