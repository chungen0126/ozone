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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileRange;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VectoredReadUtils}.
 */
class TestVectoredReadUtils {

  @Test
  void testValidateRangeRequest() throws Exception {
    // Null range
    NullPointerException npe = assertThrows(NullPointerException.class,
        () -> VectoredReadUtils.validateRangeRequest(null));
    assertTrue(npe.getMessage().contains("range is null"));

    // Negative length
    FileRange negativeLength = FileRange.createFileRange(10, -5);
    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
        () -> VectoredReadUtils.validateRangeRequest(negativeLength));
    assertTrue(iae.getMessage().contains("length is negative"));

    // Negative offset
    FileRange negativeOffset = FileRange.createFileRange(-10, 5);
    EOFException eof = assertThrows(EOFException.class,
        () -> VectoredReadUtils.validateRangeRequest(negativeOffset));
    assertTrue(eof.getMessage().contains("position is negative"));

    // Valid range
    FileRange validRange = FileRange.createFileRange(10, 5);
    FileRange validated = VectoredReadUtils.validateRangeRequest(validRange);
    assertSame(validRange, validated);
  }

  @Test
  void testSortRangeList() {
    FileRange r1 = FileRange.createFileRange(50, 10);
    FileRange r2 = FileRange.createFileRange(10, 20);
    FileRange r3 = FileRange.createFileRange(100, 5);
    List<FileRange> input = Arrays.asList(r1, r2, r3);

    List<? extends FileRange> sorted = VectoredReadUtils.sortRangeList(input);

    assertEquals(3, sorted.size());
    assertSame(r2, sorted.get(0));
    assertSame(r1, sorted.get(1));
    assertSame(r3, sorted.get(2));

    // Ensure input list is not modified
    assertSame(r1, input.get(0));
    assertSame(r2, input.get(1));
    assertSame(r3, input.get(2));
  }

  @Test
  void testValidateRanges() throws Exception {
    // Null list
    NullPointerException npe = assertThrows(NullPointerException.class,
        () -> VectoredReadUtils.validateRanges(null));
    assertTrue(npe.getMessage().contains("Null input list"));

    // Empty list
    VectoredReadUtils.validateRanges(Collections.emptyList());

    // Single valid range
    FileRange singleValid = FileRange.createFileRange(0, 10);
    VectoredReadUtils.validateRanges(Collections.singletonList(singleValid));

    // Single invalid range (negative length)
    FileRange singleInvalid = FileRange.createFileRange(0, -10);
    assertThrows(IllegalArgumentException.class,
        () -> VectoredReadUtils.validateRanges(Collections.singletonList(singleInvalid)));

    // Multiple valid non-overlapping, sorted
    FileRange r1 = FileRange.createFileRange(0, 10);
    FileRange r2 = FileRange.createFileRange(10, 5);
    FileRange r3 = FileRange.createFileRange(20, 15);
    VectoredReadUtils.validateRanges(Arrays.asList(r1, r2, r3));

    // Multiple valid non-overlapping, unsorted
    VectoredReadUtils.validateRanges(Arrays.asList(r3, r1, r2));

    // Overlapping ranges (exact boundary overlap - (0,10) and (10,5) is NOT overlapping,
    // but (0,11) and (10,5) IS overlapping)
    FileRange rOverlapping = FileRange.createFileRange(9, 5);
    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
        () -> VectoredReadUtils.validateRanges(Arrays.asList(r1, rOverlapping)));
    assertTrue(iae.getMessage().contains("Overlapping ranges"));

    // Identical ranges
    iae = assertThrows(IllegalArgumentException.class,
        () -> VectoredReadUtils.validateRanges(Arrays.asList(r1, r1)));
    assertTrue(iae.getMessage().contains("Overlapping ranges"));
  }

  @Test
  void testPerformVectoredReadSuccess() throws Exception {
    FileRange r1 = FileRange.createFileRange(0, 5);
    FileRange r2 = FileRange.createFileRange(10, 10);
    List<FileRange> ranges = Arrays.asList(r1, r2);

    VectoredReadUtils.performVectoredRead(
        ranges,
        ByteBuffer::allocate,
        (offset, buffer) -> {
          // Fill buffer with dummy data based on offset
          int len = buffer.remaining();
          for (int i = 0; i < len; i++) {
            buffer.put((byte) (offset + i));
          }
        }
    );

    // Wait for completion
    CompletableFuture<ByteBuffer> f1 = r1.getData();
    CompletableFuture<ByteBuffer> f2 = r2.getData();

    assertNotNull(f1);
    assertNotNull(f2);

    ByteBuffer b1 = f1.get(5, TimeUnit.SECONDS);
    ByteBuffer b2 = f2.get(5, TimeUnit.SECONDS);

    // Check sizes and contents
    assertEquals(5, b1.remaining());
    assertEquals(0, b1.position());
    for (int i = 0; i < 5; i++) {
      assertEquals((byte) (0 + i), b1.get());
    }

    assertEquals(10, b2.remaining());
    assertEquals(0, b2.position());
    for (int i = 0; i < 10; i++) {
      assertEquals((byte) (10 + i), b2.get());
    }
  }

  @Test
  void testPerformVectoredReadValidationFailure() {
    FileRange r1 = FileRange.createFileRange(0, 10);
    FileRange r2 = FileRange.createFileRange(5, 5); // Overlapping
    List<FileRange> ranges = Arrays.asList(r1, r2);

    assertThrows(IllegalArgumentException.class,
        () -> VectoredReadUtils.performVectoredRead(
            ranges,
            ByteBuffer::allocate,
            (offset, buffer) -> { }
        )
    );
  }

  @Test
  void testPerformVectoredReadReaderFailure() throws Exception {
    FileRange r1 = FileRange.createFileRange(0, 5);
    List<FileRange> ranges = Collections.singletonList(r1);

    VectoredReadUtils.performVectoredRead(
        ranges,
        ByteBuffer::allocate,
        (offset, buffer) -> {
          throw new IOException("Read failed");
        }
    );

    CompletableFuture<ByteBuffer> f1 = r1.getData();
    assertNotNull(f1);

    ExecutionException ee = assertThrows(ExecutionException.class,
        () -> f1.get(5, TimeUnit.SECONDS));
    assertTrue(ee.getCause() instanceof IOException);
    assertEquals("Read failed", ee.getCause().getMessage());
  }

  @Test
  void testPerformVectoredReadWithPreexistingDataFuture() throws Exception {
    FileRange r1 = FileRange.createFileRange(0, 5);
    CompletableFuture<ByteBuffer> existingFuture = new CompletableFuture<>();
    r1.setData(existingFuture);

    List<FileRange> ranges = Collections.singletonList(r1);

    VectoredReadUtils.performVectoredRead(
        ranges,
        ByteBuffer::allocate,
        (offset, buffer) -> {
          buffer.put(new byte[]{1, 2, 3, 4, 5});
        }
    );

    assertSame(existingFuture, r1.getData());
    ByteBuffer b = existingFuture.get(5, TimeUnit.SECONDS);
    assertEquals(5, b.remaining());
    assertEquals(1, b.get());
    assertEquals(2, b.get());
  }
}
