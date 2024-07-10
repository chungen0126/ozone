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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerCheck;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerChunkStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;

/**
 * Benchmark for KeyValueContainerCheck.
 */
@Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkKeyValueContainerCheck {
  private static final int UNIT_LEN = 1024;
  private static final int CHUNK_LEN = 4 * UNIT_LEN * UNIT_LEN;
  private static final int CHUNKS_PER_BLOCK = 64;
  private static final int BYTES_PER_CHECKSUM = 8 * UNIT_LEN;
  private static final int BLOCKS_PER_CONTAINER = 20;
  private static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "test" + File.separator + "data";
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final long CONTAINER_ID = 101;
  private static final DispatcherContext WRITE_STAGE =
      DispatcherContext.newBuilder(DispatcherContext.Op.WRITE_STATE_MACHINE_DATA)
          .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
          .build();

  private static final DispatcherContext COMMIT_STAGE =
      DispatcherContext.newBuilder(DispatcherContext.Op.APPLY_TRANSACTION)
          .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
          .setContainer2BCSIDMap(Collections.emptyMap())
          .build();

  /**
   * State for the benchmark.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"FILE_PER_CHUNK", "FILE_PER_BLOCK"})
    private ContainerLayoutVersion containerLayoutVersion;
    @Param({OzoneConsts.SCHEMA_V1, OzoneConsts.SCHEMA_V2, OzoneConsts.SCHEMA_V3})
    private String schema;
    private OzoneConfiguration conf;
    private MutableVolumeSet volumeSet;
    private KeyValueContainerCheck kvCheck;
    private File dir;
    private KeyValueContainer container;
    private KeyValueContainerData containerData;

    private static File getTestDir() throws IOException {
      File dir = new File(DEFAULT_TEST_DATA_DIR).getAbsoluteFile();
      Files.createDirectories(dir.toPath());
      return dir;
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
      dir = getTestDir();
      conf = new OzoneConfiguration();
      DatanodeConfiguration dc = conf.getObject(DatanodeConfiguration.class);
      if (schema == OzoneConsts.SCHEMA_V3) {
        dc.setContainerSchemaV3Enabled(true);
      } else {
        dc.setContainerSchemaV3Enabled(false);
      }
      conf.setFromObject(dc);
      conf.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, containerLayoutVersion.name());
      conf.set(HDDS_DATANODE_DIR_KEY, dir.getAbsolutePath());
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());
      volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), CLUSTER_ID,
          conf, null, StorageVolume.VolumeType.DATA_VOLUME, null);
      if (dc.getContainerSchemaV3Enabled()) {
        for (HddsVolume volume : StorageVolumeUtil.getHddsVolumesList(
            volumeSet.getVolumesList())) {
          StorageVolumeUtil.checkVolume(volume, CLUSTER_ID, CLUSTER_ID, conf,
              null, null);
        }
      }
      ChunkManager chunkManager;
      if (containerLayoutVersion == ContainerLayoutVersion.FILE_PER_CHUNK) {
        chunkManager = new FilePerChunkStrategy(true, null, null);
      } else {
        chunkManager = new FilePerBlockStrategy(true, null, null);
      }

      createContainerWithBlocks(chunkManager);
      containerData = container.getContainerData();
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
      FileUtils.deleteQuietly(dir);
    }
    private void createContainerWithBlocks(ChunkManager chunkManager)
        throws Exception {
      String strBlock = "block";
      String strChunk = "-chunkFile";
      Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256,
          BYTES_PER_CHECKSUM);
      byte[] chunkData = RandomStringUtils.randomAscii(CHUNK_LEN).getBytes(UTF_8);
      ChecksumData checksumData = checksum.computeChecksum(chunkData);

      containerData = new KeyValueContainerData(CONTAINER_ID,
          containerLayoutVersion,
          (long) CHUNKS_PER_BLOCK * CHUNK_LEN * BLOCKS_PER_CONTAINER,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
      container = new KeyValueContainer(containerData, conf);
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          CLUSTER_ID);
      try (DBHandle metadataStore = BlockUtils.getDB(containerData,
          conf)) {
        List<ChunkInfo> chunkList = new ArrayList<>();
        for (int i = 0; i < BLOCKS_PER_CONTAINER; i++) {
          BlockID blockID = new BlockID(CONTAINER_ID, i);
          BlockData blockData = new BlockData(blockID);

          chunkList.clear();
          for (long chunkCount = 0; chunkCount < CHUNKS_PER_BLOCK; chunkCount++) {
            String chunkName = strBlock + i + strChunk + chunkCount;
            long offset = chunkCount * CHUNK_LEN;
            org.apache.hadoop.ozone.container.common.helpers.ChunkInfo
                info = new org.apache.hadoop.ozone.container.common.helpers.ChunkInfo(chunkName, offset, CHUNK_LEN);
            info.setChecksumData(checksumData);
            chunkList.add(info.getProtoBufMessage());
            chunkManager.writeChunk(container, blockID, info,
                ByteBuffer.wrap(chunkData), WRITE_STAGE);
            chunkManager.writeChunk(container, blockID, info,
                ByteBuffer.wrap(chunkData), COMMIT_STAGE);
          }
          blockData.setChunks(chunkList);

          // normal key
          String key = containerData.getBlockKey(blockID.getLocalID());
          metadataStore.getStore().getBlockDataTable().put(key, blockData);
        }
      }
    }
    @Benchmark
    public void fullScan(BenchmarkState state)
        throws Exception {

      kvCheck = new KeyValueContainerCheck(containerData.getMetadataPath(), conf,
          CONTAINER_ID, containerData.getVolume(), container);
      ContainerScannerConfiguration c = conf.getObject(
          ContainerScannerConfiguration.class);
      kvCheck.fullCheck(new DataTransferThrottler(
          c.getBandwidthPerVolume()), null);
    }
  }
}
