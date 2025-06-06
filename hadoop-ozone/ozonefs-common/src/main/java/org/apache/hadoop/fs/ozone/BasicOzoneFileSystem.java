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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_USER_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_MAX_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.io.SelectorOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.http.client.utils.URIBuilder;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The minimal Ozone Filesystem implementation.
 * <p>
 * This is a basic version which doesn't extend
 * KeyProviderTokenIssuer and doesn't include statistics. It can be used
 * from older hadoop version. For newer hadoop version use the full featured
 * OzoneFileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BasicOzoneFileSystem extends FileSystem {
  static final Logger LOG =
      LoggerFactory.getLogger(BasicOzoneFileSystem.class);

  /**
   * The Ozone client for connecting to Ozone server.
   */

  private URI uri;
  private String userName;
  private Path workingDir;

  private OzoneClientAdapter adapter;

  private int listingPageSize =
      OZONE_FS_LISTING_PAGE_SIZE_DEFAULT;

  private boolean hsyncEnabled = OZONE_FS_HSYNC_ENABLED_DEFAULT;
  private boolean isRatisStreamingEnabled
      = OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED_DEFAULT;
  private int streamingAutoThreshold;

  private static final Pattern URL_SCHEMA_PATTERN =
      Pattern.compile("([^\\.]+)\\.([^\\.]+)\\.{0,1}(.*)");

  private static final String URI_EXCEPTION_TEXT = "Ozone file system URL " +
      "should be one of the following formats: " +
      "o3fs://bucket.volume/key  OR " +
      "o3fs://bucket.volume.om-host.example.com/key  OR " +
      "o3fs://bucket.volume.om-host.example.com:5678/key  OR " +
      "o3fs://bucket.volume.omServiceId/key";

  private static final int PATH_DEPTH_TO_BUCKET = 0;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    listingPageSize = conf.getInt(
        OZONE_FS_LISTING_PAGE_SIZE,
        OZONE_FS_LISTING_PAGE_SIZE_DEFAULT);
    listingPageSize = OzoneClientUtils.limitValue(listingPageSize,
        OZONE_FS_LISTING_PAGE_SIZE,
        OZONE_FS_MAX_LISTING_PAGE_SIZE);
    isRatisStreamingEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED,
        OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED_DEFAULT);
    streamingAutoThreshold = (int) OzoneConfiguration.of(conf).getStorageSize(
        OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD,
        OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT,
        StorageUnit.BYTES);
    setConf(conf);
    Preconditions.checkNotNull(name.getScheme(),
        "No scheme provided in %s", name);
    Preconditions.checkArgument(getScheme().equals(name.getScheme()),
        "Invalid scheme provided in %s", name);

    String authority = name.getAuthority();
    if (authority == null) {
      // authority is null when fs.defaultFS is not a qualified o3fs URI and
      // o3fs:/// is passed to the client. matcher will NPE if authority is null
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }

    Matcher matcher = URL_SCHEMA_PATTERN.matcher(authority);

    if (!matcher.matches()) {
      throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
    }
    String bucketStr = matcher.group(1);
    String volumeStr = matcher.group(2);
    String remaining = matcher.groupCount() == 3 ? matcher.group(3) : null;

    String omHost = null;
    int omPort = -1;
    if (!isEmpty(remaining)) {
      String[] parts = remaining.split(":");
      // Array length should be either 1(hostname or service id) or 2(host:port)
      if (parts.length > 2) {
        throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
      }
      omHost = parts[0];
      if (parts.length == 2) {
        try {
          omPort = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(URI_EXCEPTION_TEXT);
        }
      }
    }

    try {
      uri = new URIBuilder().setScheme(OZONE_URI_SCHEME)
          .setHost(authority)
          .build();
      LOG.trace("Ozone URI for ozfs initialization is {}", uri);

      ConfigurationSource source = getConfSource();
      this.hsyncEnabled = OzoneFSUtils.canEnableHsync(source, true);
      LOG.debug("hsyncEnabled = {}", hsyncEnabled);
      this.adapter =
          createAdapter(source, bucketStr,
              volumeStr, omHost, omPort);

      try {
        this.userName =
            UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        this.userName = OZONE_DEFAULT_USER;
      }
      this.workingDir = new Path(OZONE_USER_DIR, this.userName)
          .makeQualified(this.uri, this.workingDir);

    } catch (URISyntaxException ue) {
      final String msg = "Invalid Ozone endpoint " + name;
      LOG.error(msg, ue);
      throw new IOException(msg, ue);
    }
  }

  protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
      String bucketStr,
      String volumeStr, String omHost, int omPort) throws IOException {

    return new BasicOzoneClientAdapterImpl(omHost, omPort, conf,
        volumeStr, bucketStr);
  }

  protected boolean isHsyncEnabled() {
    return hsyncEnabled;
  }

  @Override
  public void close() throws IOException {
    try {
      adapter.close();
    } finally {
      super.close();
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getScheme() {
    return OZONE_URI_SCHEME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    incrementCounter(Statistic.INVOCATION_OPEN, 1);
    statistics.incrementReadOps(1);
    LOG.trace("open() path:{}", f);
    final String key = pathToKey(f);
    InputStream inputStream = adapter.readFile(key);
    return new FSDataInputStream(createFSInputStream(inputStream));
  }

  protected InputStream createFSInputStream(InputStream inputStream) {
    return new OzoneFSInputStream(inputStream, statistics);
  }

  protected void incrementCounter(Statistic statistic) {
    incrementCounter(statistic, 1);
  }

  protected void incrementCounter(Statistic statistic, long count) {
    //don't do anything in this default implementation.
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize,
      Progressable progress) throws IOException {
    LOG.trace("create() path:{}", f);
    incrementCounter(Statistic.INVOCATION_CREATE, 1);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(f);
    return createOutputStream(key, replication, overwrite, true);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    incrementCounter(Statistic.INVOCATION_CREATE_NON_RECURSIVE, 1);
    statistics.incrementWriteOps(1);
    final String key = pathToKey(path);
    return createOutputStream(key,
        replication, flags.contains(CreateFlag.OVERWRITE), false);
  }

  private OutputStream selectOutputStream(String key, short replication,
      boolean overwrite, boolean recursive, int byteWritten)
      throws IOException {
    return isRatisStreamingEnabled && byteWritten > streamingAutoThreshold ?
        createFSDataStreamOutput(adapter.createStreamFile(key, replication, overwrite, recursive))
        : createFSOutputStream(adapter.createFile(
        key, replication, overwrite, recursive));
  }

  private FSDataOutputStream createOutputStream(String key, short replication,
      boolean overwrite, boolean recursive) throws IOException {
    if (isRatisStreamingEnabled) {
      // select OutputStream type based on byteWritten
      final CheckedFunction<Integer, OutputStream, IOException> selector
          = byteWritten -> selectOutputStream(
          key, replication, overwrite, recursive, byteWritten);
      return new FSDataOutputStream(new SelectorOutputStream<>(
          streamingAutoThreshold, selector), statistics);
    }

    return new FSDataOutputStream(createFSOutputStream(
            adapter.createFile(key,
        replication, overwrite, recursive)), statistics);
  }

  protected OzoneFSOutputStream createFSOutputStream(
      OzoneFSOutputStream outputStream) {
    return outputStream;
  }

  protected OzoneFSDataStreamOutput createFSDataStreamOutput(
      OzoneFSDataStreamOutput outputDataStream) {
    return outputDataStream;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("append() Not implemented by the "
        + getClass().getSimpleName() + " FileSystem implementation");
  }

  private class RenameIterator extends OzoneListingIterator {
    private final String srcKey;
    private final String dstKey;

    RenameIterator(Path srcPath, Path dstPath)
        throws IOException {
      super(srcPath);
      srcKey = pathToKey(srcPath);
      dstKey = pathToKey(dstPath);
      LOG.trace("rename from:{} to:{}", srcKey, dstKey);
    }

    @Override
    boolean processKey(List<String> keyList) throws IOException {
      // TODO RenameKey needs to be changed to batch operation
      for (String key : keyList) {
        String newKeyName = dstKey.concat(key.substring(srcKey.length()));
        try {
          adapter.renameKey(key, newKeyName);
        } catch (OMException ome) {
          LOG.error("Key rename failed for source key: {} to " +
              "destination key: {}.", key, newKeyName, ome);
          if (OMException.ResultCodes.KEY_ALREADY_EXISTS == ome.getResult() ||
              OMException.ResultCodes.KEY_RENAME_ERROR  == ome.getResult() ||
              OMException.ResultCodes.KEY_NOT_FOUND == ome.getResult()) {
            return false;
          } else {
            throw ome;
          }
        }
      }
      return true;
    }
  }

  /**
   * Check whether the source and destination path are valid and then perform
   * rename from source path to destination path.
   * <p>
   * The rename operation is performed by renaming the keys with src as prefix.
   * For such keys the prefix is changed from src to dst.
   *
   * @param src source path for rename
   * @param dst destination path for rename
   * @return true if rename operation succeeded or
   * if the src and dst have the same path and are of the same type
   * @throws IOException on I/O errors or if the src/dst paths are invalid.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_RENAME, 1);
    statistics.incrementWriteOps(1);
    super.checkPath(src);
    super.checkPath(dst);

    String srcPath = src.toUri().getPath();
    String dstPath = dst.toUri().getPath();
    // TODO: Discuss do we need to throw exception.
    if (srcPath.equals(dstPath)) {
      return true;
    }

    LOG.trace("rename() from:{} to:{}", src, dst);
    if (src.isRoot()) {
      // Cannot rename root of file system
      LOG.trace("Cannot rename the root of a filesystem");
      return false;
    }

    if (adapter.isFSOptimizedBucket()) {
      String srcKey = pathToKey(src);
      String dstKey = pathToKey(dst);
      return renameFSO(srcKey, dstKey);
    }

    // Check if the source exists
    FileStatus srcStatus;
    try {
      srcStatus = getFileStatus(src);
    } catch (FileNotFoundException fnfe) {
      // source doesn't exist, return
      return false;
    }

    // Cannot rename a directory to its own subdirectory
    if (srcStatus.isDirectory()) {
      Path dstParent = dst.getParent();
      while (dstParent != null && !src.equals(dstParent)) {
        dstParent = dstParent.getParent();
      }
      Preconditions.checkArgument(dstParent == null,
          "Cannot rename a directory to its own subdirectory");
    }
    // Check if the destination exists
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (FileNotFoundException fnde) {
      dstStatus = null;
    }

    if (dstStatus == null) {
      // If dst doesn't exist, check whether dst parent dir exists or not
      // if the parent exists, the source can still be renamed to dst path
      dstStatus = getFileStatus(dst.getParent());
      if (!dstStatus.isDirectory()) {
        throw new IOException(String.format(
            "Failed to rename %s to %s, %s is a file", src, dst,
            dst.getParent()));
      }
    } else {
      // if dst exists and source and destination are same,
      // check both the src and dst are of same type
      if (srcStatus.getPath().equals(dstStatus.getPath())) {
        return !srcStatus.isDirectory();
      } else if (dstStatus.isDirectory()) {
        // If dst is a directory, rename source as subpath of it.
        // for example rename /source to /dst will lead to /dst/source
        dst = new Path(dst, src.getName());
        dstPath = dst.toUri().getPath();
        FileStatus[] statuses;
        try {
          statuses = listStatus(dst);
        } catch (FileNotFoundException fnde) {
          statuses = null;
        }

        if (statuses != null && statuses.length > 0) {
          // If dst exists and not a directory not empty
          LOG.warn("Failed to rename {} to {}, file already exists" +
              " or not empty!", src, dst);
          return false;
        }
      } else {
        // If dst is not a directory
        LOG.warn("Failed to rename {} to {}, file already exists!", src, dst);
        return false;
      }
    }

    if (srcStatus.isDirectory()) {
      if (dstPath.toString()
          .startsWith(srcPath.toString() + OZONE_URI_DELIMITER)) {
        LOG.trace("Cannot rename a directory to a subdirectory of self");
        return false;
      }
    }
    RenameIterator iterator = new RenameIterator(src, dst);
    boolean result = iterator.iterate();
    if (result) {
      createFakeParentDirectory(src);
    }
    return result;
  }

  private boolean renameFSO(String srcPath, String dstPath)
      throws IOException {
    try {
      adapter.renameKey(srcPath, dstPath);
    } catch (OMException ome) {
      LOG.error("rename key failed: {}. Error code: {} source:{}, destin:{}",
              ome.getMessage(), ome.getResult(), srcPath, dstPath);
      if (OMException.ResultCodes.KEY_ALREADY_EXISTS == ome.getResult() ||
          OMException.ResultCodes.KEY_RENAME_ERROR  == ome.getResult() ||
          OMException.ResultCodes.KEY_NOT_FOUND == ome.getResult()) {
        return false;
      } else {
        throw ome;
      }
    }
    return true;
  }

  /**
   * Intercept rename to trash calls from TrashPolicyDefault.
   */
  @Deprecated
  @Override
  protected void rename(final Path src, final Path dst,
      final Rename... options) throws IOException {
    boolean hasMoveToTrash = false;
    if (options != null) {
      for (Rename option : options) {
        if (option == Rename.TO_TRASH) {
          hasMoveToTrash = true;
          break;
        }
      }
    }
    if (!hasMoveToTrash) {
      // if doesn't have TO_TRASH option, just pass the call to super
      super.rename(src, dst, options);
    } else {
      rename(src, dst);
    }
  }

  private class DeleteIterator extends OzoneListingIterator {
    private boolean recursive;

    DeleteIterator(Path f, boolean recursive)
        throws IOException {
      super(f);
      this.recursive = recursive;
      if (getStatus().isDirectory()
          && !this.recursive
          && listStatus(f).length != 0) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
    }

    @Override
    boolean processKey(List<String> key) throws IOException {
      LOG.trace("deleting key:{}", key);
      boolean succeed = adapter.deleteObjects(key);
      // if recursive delete is requested ignore the return value of
      // deleteObject and issue deletes for other keys.
      return recursive || succeed;
    }
  }

  /**
   * Deletes the children of the input dir path by iterating though the
   * DeleteIterator.
   *
   * @param f directory path to be deleted
   * @return true if successfully deletes all required keys, false otherwise
   * @throws IOException
   */
  private boolean innerDelete(Path f, boolean recursive) throws IOException {
    LOG.trace("delete() path:{} recursive:{}", f, recursive);
    try {
      DeleteIterator iterator = new DeleteIterator(f, recursive);

      if (f.isRoot()) {
        LOG.warn("Cannot delete root directory.");
        return false;
      }

      return iterator.iterate();
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't delete {} - does not exist", f);
      }
      return false;
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    incrementCounter(Statistic.INVOCATION_DELETE, 1);
    statistics.incrementWriteOps(1);
    LOG.debug("Delete path {} - recursive {}", f, recursive);

    if (adapter.isFSOptimizedBucket()) {
      if (f.isRoot()) {
        if (!recursive && listStatus(f).length != 0) {
          throw new PathIsNotEmptyDirectoryException(f.toString());
        }
        LOG.warn("Cannot delete root directory.");
        return false;
      }

      String key = pathToKey(f);
      return adapter.deleteObject(key, recursive);
    }

    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException ex) {
      LOG.warn("delete: Path does not exist: {}", f);
      return false;
    }

    String key = pathToKey(f);
    boolean result;

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {}", f);

      result = innerDelete(f, recursive);
    } else {
      LOG.debug("delete: Path is a file: {}", f);
      result = adapter.deleteObject(key);
    }

    if (result) {
      // If this delete operation removes all files/directories from the
      // parent directory, then an empty parent directory must be created.
      createFakeParentDirectory(f);
    }

    return result;
  }

  /**
   * Create a fake parent directory key if it does not already exist and no
   * other child of this parent directory exists.
   *
   * @param f path to the fake parent directory
   * @throws IOException
   */
  private void createFakeParentDirectory(Path f) throws IOException {
    Path parent = f.getParent();
    if (parent != null && !parent.isRoot()) {
      createFakeDirectoryIfNecessary(parent);
    }
  }

  /**
   * Create a fake directory key if it does not already exist.
   *
   * @param f path to the fake directory
   * @throws IOException
   */
  private void createFakeDirectoryIfNecessary(Path f) throws IOException {
    String key = pathToKey(f);
    if (!key.isEmpty() && !o3Exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      String dirKey = addTrailingSlashIfNeeded(key);
      adapter.createDirectory(dirKey);
    }
  }

  /**
   * Check if a file or directory exists corresponding to given path.
   *
   * @param f path to file/directory.
   * @return true if it exists, false otherwise.
   * @throws IOException
   */
  private boolean o3Exists(final Path f) throws IOException {
    Path path = makeQualified(f);
    try {
      getFileStatus(path);
      return true;
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("listStatus() path:{}", f);
    int numEntries = listingPageSize;
    LinkedList<FileStatus> statuses = new LinkedList<>();
    List<FileStatus> tmpStatusList;
    String startKey = "";
    int entriesAdded;
    do {
      tmpStatusList =
          adapter.listStatus(pathToKey(f), false, startKey, numEntries, uri,
                  workingDir, getUsername(), true)
              .stream()
              .map(this::convertFileStatus)
              .collect(Collectors.toList());
      entriesAdded = 0;
      if (!tmpStatusList.isEmpty()) {
        if (startKey.isEmpty() || !statuses.getLast().getPath().toString()
            .equals(tmpStatusList.get(0).getPath().toString())) {
          statuses.addAll(tmpStatusList);
          entriesAdded += tmpStatusList.size();
        } else {
          statuses.addAll(tmpStatusList.subList(1, tmpStatusList.size()));
          entriesAdded += tmpStatusList.size() - 1;
        }
        startKey = pathToKey(statuses.getLast().getPath());
      }
      // listStatus returns entries numEntries in size if available.
      // Any lesser number of entries indicate that the required entries have
      // exhausted.
    } while (entriesAdded > 0);


    return statuses.toArray(new FileStatus[0]);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(OZONE_USER_DIR + "/"
        + this.userName));
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return adapter.getDelegationToken(renewer);
  }

  /**
   * Get a canonical service name for this file system. If the URI is logical,
   * the hostname part of the URI will be returned.
   *
   * @return a service string that uniquely identifies this file system.
   */
  @Override
  public String getCanonicalServiceName() {
    return adapter.getCanonicalServiceName();
  }

  /**
   * Get the username of the FS.
   *
   * @return the short name of the user who instantiated the FS
   */
  public String getUsername() {
    return userName;
  }

  /**
   * Get the root directory of Trash for a path.
   * Returns /.Trash/<username>
   * Caller appends either Current or checkpoint timestamp for trash destination
   * @param path the trash root of the path to be determined.
   * @return trash root
   */
  @Override
  public Path getTrashRoot(Path path) {
    final Path pathToTrash = new Path(OZONE_URI_DELIMITER, TRASH_PREFIX);
    return this.makeQualified(new Path(pathToTrash, getUsername()));
  }

  /**
   * Get all the trash roots for current user or all users.
   *
   * @param allUsers return trash roots for all users if true.
   * @return all the trash root directories.
   *         Returns .Trash of users if {@code /.Trash/$USER} exists.
   */
  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    Path trashRoot = new Path(OZONE_URI_DELIMITER, TRASH_PREFIX);
    List<FileStatus> ret = new ArrayList<>();
    try {
      if (!allUsers) {
        Path userTrash = new Path(trashRoot, userName);
        if (exists(userTrash) && getFileStatus(userTrash).isDirectory()) {
          ret.add(getFileStatus(userTrash));
        }
      } else {
        if (exists(trashRoot)) {
          FileStatus[] candidates = listStatus(trashRoot);
          for (FileStatus candidate : candidates) {
            if (candidate.isDirectory()) {
              ret.add(candidate);
            }
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Can't get all trash roots", ex);
    }
    return ret;
  }

  /**
   * Creates a directory. Directory is represented using a key with no value.
   *
   * @param path directory path to be created
   * @return true if directory exists or created successfully.
   * @throws IOException
   */
  private boolean mkdir(Path path) throws IOException {
    return adapter.createDirectory(pathToKey(path));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    incrementCounter(Statistic.INVOCATION_MKDIRS);
    LOG.trace("mkdir() path:{} ", f);
    String key = pathToKey(f);
    if (isEmpty(key)) {
      return false;
    }
    return mkdir(f);
  }

  @Override
  public long getDefaultBlockSize() {
    return (long)getConfSource().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("getFileStatus() path:{}", f);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);
    FileStatus fileStatus = null;
    try {
      fileStatus = convertFileStatus(
          adapter.getFileStatus(key, uri, qualifiedPath, getUsername()));
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        if (((OMException) ex).getResult()
            .equals(OMException.ResultCodes.KEY_NOT_FOUND)) {
          throw new FileNotFoundException("File not found. path:" + f);
        }
      }
      throw ex;
    }
    return fileStatus;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fileStatus,
                                               long start, long len)
      throws IOException {
    if (fileStatus instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) fileStatus).getBlockLocations();
    } else {
      return super.getFileBlockLocations(fileStatus, start, len);
    }
  }

  @Override
  public short getDefaultReplication() {
    return adapter.getDefaultReplication();
  }

  @Override
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    return adapter.getServerDefaults();
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs,
      Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
      Path dst) throws IOException {
    incrementCounter(Statistic.INVOCATION_COPY_FROM_LOCAL_FILE);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_EXISTS);
    return super.exists(f);
  }

  @Override
  public FileChecksum getFileChecksum(Path f, long length) throws IOException {
    incrementCounter(Statistic.INVOCATION_GET_FILE_CHECKSUM);
    statistics.incrementReadOps(1);
    Path qualifiedPath = f.makeQualified(uri, workingDir);
    String key = pathToKey(qualifiedPath);
    return adapter.getFileChecksum(key, length);
  }

  @Override
  protected Path fixRelativePart(Path p) {
    String pathPatternString = p.toUri().getPath();
    if (pathPatternString.isEmpty()) {
      return new Path("/");
    } else {
      return super.fixRelativePart(p);
    }
  }
   
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    incrementCounter(Statistic.INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_GLOB_STATUS);
    return super.globStatus(pathPattern, filter);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isDirectory(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_DIRECTORY);
    return super.isDirectory(f);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isFile(Path f) throws IOException {
    incrementCounter(Statistic.INVOCATION_IS_FILE);
    return super.isFile(f);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_FILES);
    return super.listFiles(f, recursive);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_LOCATED_STATUS);
    return new OzoneFileStatusIterator<>(f,
        (stat) -> stat instanceof LocatedFileStatus ? (LocatedFileStatus) stat : new LocatedFileStatus(stat, null),
        false);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f)
      throws IOException {
    return new OzoneFileStatusIterator<>(f, stat -> stat, true);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
          throws IOException {
    String snapshot = getAdapter()
        .createSnapshot(pathToKey(path), snapshotName);
    return new Path(OzoneFSUtils.trimPathToDepth(path, PATH_DEPTH_TO_BUCKET),
        OM_SNAPSHOT_INDICATOR + OZONE_URI_DELIMITER + snapshot);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
      throws IOException {
    getAdapter().renameSnapshot(pathToKey(path), snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    adapter.deleteSnapshot(pathToKey(path), snapshotName);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    incrementCounter(Statistic.INVOCATION_SET_TIMES, 1);
    statistics.incrementWriteOps(1);
    LOG.trace("setTimes() path:{}", f);
    Path qualifiedPath = makeQualified(f);
    String key = pathToKey(qualifiedPath);
    adapter.setTimes(key, mtime, atime);
  }

  /**
   * A private class implementation for iterating list of file status.
   *
   * @param <T> the type of the file status.
   */
  private final class OzoneFileStatusIterator<T extends FileStatus>
      implements RemoteIterator<T> {
    private List<FileStatus> thisListing;
    private int i;
    private Path p;
    private T curStat = null;
    private String startPath = "";
    private boolean lite;
    private Function<FileStatus, T> transformFunc;

    /**
     * Constructor to initialize OzoneFileStatusIterator.
     * Get the first batch of entry for iteration.
     *
     * @param p path to file/directory.
     * @param transformFunc function to convert FileStatus into an expected type.
     * @param lite if true it should look into fetching a lightweight keys from server.
     * @throws IOException
     */
    private OzoneFileStatusIterator(Path p, Function<FileStatus, T> transformFunc, boolean lite) throws IOException {
      this.p = p;
      this.lite = lite;
      this.transformFunc = transformFunc;
      // fetch the first batch of entries in the directory
      thisListing = listFileStatus(p, startPath, lite);
      if (thisListing != null && !thisListing.isEmpty()) {
        startPath = pathToKey(
            thisListing.get(thisListing.size() - 1).getPath());
        LOG.debug("Got {} file status, next start path {}",
            thisListing.size(), startPath);
      }
      i = 0;
    }

    /**
     * @return true if next entry exists false otherwise.
     * @throws IOException
     */
    @Override
    public boolean hasNext() throws IOException {
      while (curStat == null && hasNextNoFilter()) {
        T next;
        FileStatus fileStat = thisListing.get(i++);
        next = this.transformFunc.apply(fileStat);
        curStat = next;
      }
      return curStat != null;
    }

    /**
     * Checks the next available entry from partial listing if not exhausted
     * or fetches new batch for listing.
     *
     * @return true if next entry exists false otherwise.
     * @throws IOException
     */
    private boolean hasNextNoFilter() throws IOException {
      if (thisListing == null) {
        return false;
      }
      if (i >= thisListing.size()) {
        if (startPath != null && (!thisListing.isEmpty())) {
          // current listing is exhausted & fetch a new listing
          thisListing = listFileStatus(p, startPath, lite);
          if (thisListing != null && !thisListing.isEmpty()) {
            startPath = pathToKey(
                thisListing.get(thisListing.size() - 1).getPath());
            LOG.debug("Got {} file status, next start path {}",
                thisListing.size(), startPath);
          } else {
            return false;
          }
          i = 0;
        }
      }
      return (i < thisListing.size());
    }

    /**
     * @return next entry.
     * @throws IOException
     */
    @Override
    public T next() throws IOException {
      if (hasNext()) {
        T tmp = curStat;
        curStat = null;
        return tmp;
      }
      throw new java.util.NoSuchElementException("No more entry in " + p);
    }
  }

  /**
   * Get all the file status for input path and startPath.
   *
   * @param f
   * @param startPath
   * @param lite if true return lightweight keys
   * @return list of file status.
   * @throws IOException
   */
  private List<FileStatus> listFileStatus(Path f, String startPath, boolean lite)
      throws IOException {
    incrementCounter(Statistic.INVOCATION_LIST_STATUS, 1);
    statistics.incrementReadOps(1);
    LOG.trace("listFileStatus() path:{}", f);
    List<FileStatus> statusList;
    statusList =
        adapter.listStatus(pathToKey(f), false, startPath,
                listingPageSize, uri, workingDir, getUsername(), lite)
            .stream()
            .map(this::convertFileStatus)
            .collect(Collectors.toList());

    if (!statusList.isEmpty() && !startPath.isEmpty()) {
      // Excluding the 1st file status element from list.
      statusList.remove(0);
    }
    return statusList;
  }

  /**
   * Turn a path (relative or otherwise) into an Ozone key.
   *
   * @param path the path of the file.
   * @return the key of the object that represents the file.
   */
  public String pathToKey(Path path) {
    Objects.requireNonNull(path, "Path can not be null!");
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }
    // removing leading '/' char
    String key = path.toUri().getPath();

    if (!OzoneFSUtils.isValidName(key)) {
      throw new InvalidPathException("Invalid path Name " + key);
    }
    LOG.trace("path for key:{} is:{}", key, path);
    return key.substring(1);
  }

  /**
   * Add trailing delimiter to path if it is already not present.
   *
   * @param key the ozone Key which needs to be appended
   * @return delimiter appended key
   */
  private String addTrailingSlashIfNeeded(String key) {
    if (!isEmpty(key) && !key.endsWith(OZONE_URI_DELIMITER)) {
      return key + OZONE_URI_DELIMITER;
    } else {
      return key;
    }
  }

  public ConfigurationSource getConfSource() {
    Configuration conf = super.getConf();
    ConfigurationSource source;
    if (conf instanceof OzoneConfiguration) {
      source = (ConfigurationSource) conf;
    } else {
      source = new LegacyHadoopConfigurationSource(conf);
    }
    return source;
  }

  @Override
  public String toString() {
    return "OzoneFileSystem{URI=" + uri + ", "
        + "workingDir=" + workingDir + ", "
        + "userName=" + userName + ", "
        + "statistics=" + statistics
        + "}";
  }

  /**
   * This class provides an interface to iterate through all the keys in the
   * bucket prefixed with the input path key and process them.
   * <p>
   * Each implementing class should define how the keys should be processed
   * through the processKey() function.
   */
  private abstract class OzoneListingIterator {
    private final Path path;
    private final FileStatus status;
    private String pathKey;
    private Iterator<BasicKeyInfo> keyIterator;

    OzoneListingIterator(Path path)
        throws IOException {
      this.path = path;
      this.status = getFileStatus(path);
      this.pathKey = pathToKey(path);
      if (status.isDirectory()) {
        this.pathKey = addTrailingSlashIfNeeded(pathKey);
      }
      keyIterator = adapter.listKeys(pathKey);
    }

    /**
     * The output of processKey determines if further iteration through the
     * keys should be done or not.
     *
     * @return true if we should continue iteration of keys, false otherwise.
     * @throws IOException
     */
    abstract boolean processKey(List<String> key) throws IOException;

    /**
     * Iterates thorugh all the keys prefixed with the input path's key and
     * processes the key though processKey().
     * If for any key, the processKey() returns false, then the iteration is
     * stopped and returned with false indicating that all the keys could not
     * be processed successfully.
     *
     * @return true if all keys are processed successfully, false otherwise.
     * @throws IOException
     */
    boolean iterate() throws IOException {
      LOG.trace("Iterating path {}", path);
      List<String> keyList = new ArrayList<>();
      int batchSize = getConf().getInt(OZONE_FS_ITERATE_BATCH_SIZE,
          OZONE_FS_ITERATE_BATCH_SIZE_DEFAULT);
      if (status.isDirectory()) {
        LOG.trace("Iterating directory:{}", pathKey);
        while (keyIterator.hasNext()) {
          BasicKeyInfo key = keyIterator.next();
          LOG.trace("iterating key:{}", key.getName());
          if (!key.getName().equals("")) {
            keyList.add(key.getName());
          }
          if (keyList.size() >= batchSize) {
            if (!processKey(keyList)) {
              return false;
            } else {
              keyList.clear();
            }
          }
        }
        if (!keyList.isEmpty()) {
          if (!processKey(keyList)) {
            return false;
          }
        }
        return true;
      } else {
        LOG.trace("iterating file:{}", path);
        keyList.add(pathKey);
        return processKey(keyList);
      }
    }

    String getPathKey() {
      return pathKey;
    }

    boolean pathIsDirectory() {
      return status.isDirectory();
    }

    FileStatus getStatus() {
      return status;
    }
  }

  public OzoneClientAdapter getAdapter() {
    return adapter;
  }

  public boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  public boolean isNumber(String number) {
    try {
      Integer.parseInt(number);
    } catch (NumberFormatException ex) {
      return false;
    }
    return true;
  }

  protected FileStatus constructFileStatus(
          FileStatusAdapter fileStatusAdapter) {
    return new FileStatus(fileStatusAdapter.getLength(),
            fileStatusAdapter.isDir(),
            fileStatusAdapter.getBlockReplication(),
            fileStatusAdapter.getBlocksize(),
            fileStatusAdapter.getModificationTime(),
            fileStatusAdapter.getAccessTime(),
            new FsPermission(fileStatusAdapter.getPermission()),
            fileStatusAdapter.getOwner(),
            fileStatusAdapter.getGroup(),
            fileStatusAdapter.getSymlink(),
            fileStatusAdapter.getPath(),
            false,
            fileStatusAdapter.isEncrypted(),
            fileStatusAdapter.isErasureCoded()
    );
  }

  private FileStatus convertFileStatus(
      FileStatusAdapter fileStatusAdapter) {
    FileStatus fileStatus = constructFileStatus(fileStatusAdapter);

    BlockLocation[] blockLocations = fileStatusAdapter.getBlockLocations();
    if (blockLocations.length == 0) {
      return fileStatus;
    }
    return new LocatedFileStatus(fileStatus, blockLocations);
  }

  protected boolean setSafeModeUtil(SafeModeAction action,
      boolean isChecked)
      throws IOException {
    if (action == SafeModeAction.GET) {
      statistics.incrementReadOps(1);
    } else {
      statistics.incrementWriteOps(1);
    }
    LOG.trace("setSafeMode() action:{}", action);
    return getAdapter().setSafeMode(action, isChecked);
  }
}
