/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.List;

/**
 * Metadata object that represents a Datanode.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class DatanodeMetadata {

  @XmlElement(name = "uuid")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String uuid;

  @XmlElement(name = "hostname")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String hostname;

  @XmlElement(name = "state")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private NodeState state;

  @XmlElement(name = "opState")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private NodeOperationalState opState;

  @XmlElement(name = "lastHeartbeat")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long lastHeartbeat;

  @XmlElement(name = "storageReport")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private DatanodeStorageReport datanodeStorageReport;

  @XmlElement(name = "pipelines")
  private List<DatanodePipeline> pipelines;

  @XmlElement(name = "containers")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int containers;

  @XmlElement(name = "openContainers")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int openContainers;

  @XmlElement(name = "leaderCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int leaderCount;

  @XmlElement(name = "version")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String version;

  @XmlElement(name = "setupTime")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long setupTime;

  @XmlElement(name = "revision")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String revision;

  @XmlElement(name = "layoutVersion")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int layoutVersion;

  @XmlElement(name = "networkLocation")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String networkLocation;

  private DatanodeMetadata(Builder builder) {
    this.hostname = builder.hostname;
    this.uuid = builder.uuid;
    this.state = builder.state;
    this.opState = builder.opState;
    this.lastHeartbeat = builder.lastHeartbeat;
    this.datanodeStorageReport = builder.datanodeStorageReport;
    this.pipelines = builder.pipelines;
    this.containers = builder.containers;
    this.openContainers = builder.openContainers;
    this.leaderCount = builder.leaderCount;
    this.version = builder.version;
    this.setupTime = builder.setupTime;
    this.revision = builder.revision;
    this.layoutVersion = builder.layoutVersion;
    this.networkLocation = builder.networkLocation;
  }

  public String getHostname() {
    return hostname;
  }

  public NodeState getState() {
    return state;
  }

  public NodeOperationalState getOperationalState() {
    return opState;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public DatanodeStorageReport getDatanodeStorageReport() {
    return datanodeStorageReport;
  }

  public List<DatanodePipeline> getPipelines() {
    return pipelines;
  }

  public int getContainers() {
    return containers;
  }

  public int getOpenContainers() {
    return openContainers;
  }

  public int getLeaderCount() {
    return leaderCount;
  }

  public String getUuid() {
    return uuid;
  }

  public String getVersion() {
    return version;
  }

  public long getSetupTime() {
    return  setupTime;
  }

  public String getRevision() {
    return revision;
  }

  public int getLayoutVersion() {
    return layoutVersion;
  }

  public String getNetworkLocation() {
    return networkLocation;
  }

  /**
   * Returns new builder class that builds a DatanodeMetadata.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for DatanodeMetadata.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String hostname;
    private String uuid;
    private NodeState state;
    private NodeOperationalState opState;
    private long lastHeartbeat;
    private DatanodeStorageReport datanodeStorageReport;
    private List<DatanodePipeline> pipelines;
    private int containers;
    private int openContainers;
    private int leaderCount;
    private String version;
    private long setupTime;
    private String revision;
    private int layoutVersion;
    private String networkLocation;

    public Builder() {
      this.containers = 0;
      this.openContainers = 0;
      this.leaderCount = 0;
    }

    public Builder withHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder withState(NodeState state) {
      this.state = state;
      return this;
    }

    public Builder withOperationalState(NodeOperationalState opState) {
      this.opState = opState;
      return this;
    }

    public Builder withLastHeartbeat(long lastHeartbeat) {
      this.lastHeartbeat = lastHeartbeat;
      return this;
    }

    public Builder withDatanodeStorageReport(DatanodeStorageReport 
                                                 datanodeStorageReport) {
      this.datanodeStorageReport = datanodeStorageReport;
      return this;
    }

    public Builder withPipelines(List<DatanodePipeline> pipelines) {
      this.pipelines = pipelines;
      return this;
    }

    public Builder withContainers(int containers) {
      this.containers = containers;
      return this;
    }

    public Builder withOpenContainers(int openContainers) {
      this.openContainers = openContainers;
      return this;
    }

    public Builder withLeaderCount(int leaderCount) {
      this.leaderCount = leaderCount;
      return this;
    }

    public Builder withUUid(String uuid) {
      this.uuid = uuid;
      return this;
    }

    public Builder withVersion(String version) {
      this.version = version;
      return this;
    }

    public Builder withSetupTime(long setupTime) {
      this.setupTime = setupTime;
      return this;
    }

    public Builder withRevision(String revision) {
      this.revision = revision;
      return this;
    }

    public Builder withLayoutVersion(int layoutVersion) {
      this.layoutVersion = layoutVersion;
      return this;
    }

    public Builder withNetworkLocation(String networkLocation) {
      this.networkLocation = networkLocation;
      return this;
    }
    /**
     * Constructs DatanodeMetadata.
     *
     * @return instance of DatanodeMetadata.
     */
    public DatanodeMetadata build() {
      Preconditions.checkNotNull(hostname);
      Preconditions.checkNotNull(state);

      return new DatanodeMetadata(this);
    }
  }
}
