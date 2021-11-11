/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.FlinkCreateHandle;
import org.apache.hudi.io.FlinkMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.FlinkCompactHelpers;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T extends HoodieRecordPayload> extends
    AbstractHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkWriteClient.class);

  /**
   * FileID to write handle mapping in order to record the write handles for each file group,
   * so that we can append the mini-batch data buffer incrementally.
   */
  private Map<String, HoodieWriteHandle<?, ?, ?, ?>> bucketToHandles;

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, false);
  }

  @Deprecated
  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    super(context, writeConfig);
    this.bucketToHandles = new HashMap<>();
  }

  @Deprecated
  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                                Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  protected HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createIndex(HoodieWriteConfig writeConfig) {
    return FlinkHoodieIndex.createIndex((HoodieFlinkEngineContext) context, config);
  }

  @Override
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, context, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap operation is not supported yet");
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    final HoodieRecord<T> record = records.get(0);
    final boolean isDelta = table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ);
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(record, isDelta, getConfig(),
        instantTime, table, record.getPartitionPath(), records.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).upsert(context, writeHandle, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("UpsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    // 创建HoodieTable
    // 根据table类型（MOR或COW），创建HoodieFlinkMergeOnReadTable或HoodieFlinkCopyOnWriteTable
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
    // 检查要写入数据的schema和table的schema是否匹配
    table.validateUpsertSchema();
    // 执行预写入操作，给writeClient设置operationType
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    // create the write handle if not exists
    // 如果HoodieWriteHandle不存在，创建一个
    final HoodieRecord<T> record = records.get(0);
    final boolean isDelta = table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ);
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(record, isDelta, getConfig(),
        instantTime, table, record.getPartitionPath(), records.listIterator());
    // 调用table的insert方法，将数据插入
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).insert(context, writeHandle, instantTime, records);
    // 如果记录了索引查找耗时，更新监控仪表盘
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    // 执行写入后操作，更新监控仪表数据并返回写入状态
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("InsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context,instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                        String instantTime,
                                        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    return result.getWriteStatuses();
  }

  @Override
  public void commitCompaction(
      String compactionInstantTime,
      List<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieCommitMetadata metadata = FlinkCompactHelpers.newInstance().createCompactionMetadata(
        table, compactionInstantTime, writeStatuses, config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  @Override
  public void completeCompaction(
      HoodieCommitMetadata metadata,
      List<WriteStatus> writeStatuses,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    finalizeWrite(table, compactionCommitTime, writeStats);
    LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
    FlinkCompactHelpers.newInstance().completeInflightCompaction(table, compactionCommitTime, metadata);

    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  @Override
  protected List<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    throw new HoodieNotSupportedException("Compaction is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    new FlinkUpgradeDowngrade(metaClient, config, context).run(metaClient, HoodieTableVersion.current(), config, context, instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  /**
   * Clean the write handles within a checkpoint interval, this operation
   * would close the underneath file handles.
   */
  public void cleanHandles() {
    this.bucketToHandles.values().forEach(handle -> {
      ((MiniBatchHandle) handle).finishWrite();
    });
    this.bucketToHandles.clear();
  }

  /**
   * Get or create a new write handle in order to reuse the file handles.
   *
   * @param record        The first record in the bucket
   * @param isDelta       Whether the table is in MOR mode
   * @param config        Write config
   * @param instantTime   The instant time
   * @param table         The table
   * @param partitionPath Partition path
   * @param recordItr     Record iterator
   * @return Existing write handle or create a new one
   */
  // table的insert操作逻辑和`writeHandle`的类型有关，所以我们先分析`getOrCreateWriteHandle`方法，
  // 了解下Hudi会根据表类型和操作类型创建出什么种类的`writeHandle`。
  private HoodieWriteHandle<?, ?, ?, ?> getOrCreateWriteHandle(
      HoodieRecord<T> record,
      boolean isDelta,
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String partitionPath,
      Iterator<HoodieRecord<T>> recordItr) {
    final HoodieRecordLocation loc = record.getCurrentLocation();
    final String fileID = loc.getFileId();
    // bucketToHandles保存了file和handle的对应关系
    if (bucketToHandles.containsKey(fileID)) {
      // 如果找到file对应的handle，获取它
      return bucketToHandles.get(fileID);
    }
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle;
    /**
     * 这里Hudi可能创建出的`writeHandle`有如下四种：
     *
     * - FlinkCreateHandle：增量写入
     * - FlinkMergeAndReplaceHandle：增量合并写入（复写旧文件）
     * - FlinkAppendHandle：追加写入（MOR表）
     * - FlinkMergeHandle：增量合并写入（滚动写入新文件）
     */
    if (isDelta) {
      // 如果表为MERGE_ON_READ类型
      // 使用FlinkAppendHandle
      writeHandle = new FlinkAppendHandle<>(config, instantTime, table, partitionPath, fileID, recordItr,
          table.getTaskContextSupplier());
    } else if (loc.getInstantTime().equals("I")) {
      // 如果是新插入的数据，使用FlinkCreateHandle
      writeHandle = new FlinkCreateHandle<>(config, instantTime, table, partitionPath,
          fileID, table.getTaskContextSupplier());
    } else {
      // 否则使用FlinkMergeHandle
      writeHandle = new FlinkMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
          fileID, table.getTaskContextSupplier());
    }
    // 加入对应关系中
    this.bucketToHandles.put(fileID, writeHandle);
    return writeHandle;
  }

  private HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context, metaClient);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  public List<String> getInflightsAndRequestedInstants(String commitType) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieTimeline unCompletedTimeline = table.getMetaClient().getCommitsTimeline().filterInflightsAndRequested();
    return unCompletedTimeline.getInstants().filter(x -> x.getAction().equals(commitType)).map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }

  public String getInflightAndRequestedInstant(String tableType) {
    final String commitType = CommitUtils.getCommitActionType(HoodieTableType.valueOf(tableType));
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieTimeline unCompletedTimeline = table.getMetaClient().getCommitsTimeline().filterInflightsAndRequested();
    return unCompletedTimeline.getInstants()
        .filter(x -> x.getAction().equals(commitType))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList()).stream()
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  public String getLastCompletedInstant(String tableType) {
    final String commitType = CommitUtils.getCommitActionType(HoodieTableType.valueOf(tableType));
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieTimeline completedTimeline = table.getMetaClient().getCommitsTimeline().filterCompletedInstants();
    return completedTimeline.getInstants()
        .filter(x -> x.getAction().equals(commitType))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList()).stream()
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  public void deletePendingInstant(String tableType, String instant) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    String commitType = CommitUtils.getCommitActionType(HoodieTableType.valueOf(tableType));
    HoodieActiveTimeline activeTimeline = table.getMetaClient().getActiveTimeline();
    activeTimeline.deletePending(HoodieInstant.State.INFLIGHT, commitType, instant);
    activeTimeline.deletePending(HoodieInstant.State.REQUESTED, commitType, instant);
  }

  public void transitionRequestedToInflight(String tableType, String inFlightInstant) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitType = CommitUtils.getCommitActionType(HoodieTableType.valueOf(tableType));
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitType, inFlightInstant);
    activeTimeline.transitionRequestedToInflight(requested, Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());
  }

  public void rollbackInflightCompaction(HoodieInstant inflightInstant) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      rollbackInflightCompaction(inflightInstant, table);
    }
  }

  public HoodieFlinkTable<T> getHoodieTable() {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }
}
