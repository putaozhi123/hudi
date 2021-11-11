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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.FlinkLazyInsertIterable;
import org.apache.hudi.io.ExplicitWriteHandleFactory;
import org.apache.hudi.io.FlinkMergeHandle;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * With {@code org.apache.hudi.operator.partitioner.BucketAssigner}, each hoodie record
 * is tagged with a bucket ID (partition path + fileID) in streaming way. All the records consumed by this
 * executor should be tagged with bucket IDs and belong to one data bucket.
 *
 * <p>These bucket IDs make it possible to shuffle the records first by the bucket ID
 * (see org.apache.hudi.operator.partitioner.BucketAssignerFunction), and this executor
 * only needs to handle the data buffer that belongs to one data bucket once at a time. So there is no need to
 * partition the buffer.
 *
 * <p>Computing the records batch locations all at a time is a pressure to the engine,
 * we should avoid that in streaming system.
 */
public abstract class BaseFlinkCommitActionExecutor<T extends HoodieRecordPayload> extends
    BaseCommitActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, HoodieWriteMetadata> {

  private static final Logger LOG = LogManager.getLogger(BaseFlinkCommitActionExecutor.class);

  protected HoodieWriteHandle<?, ?, ?, ?> writeHandle;

  public BaseFlinkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType) {
    this(context, writeHandle, config, table, instantTime, operationType, Option.empty());
  }

  public BaseFlinkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType,
                                       Option extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
    this.writeHandle = writeHandle;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute(List<HoodieRecord<T>> inputRecords) {
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();

    List<WriteStatus> writeStatuses = new LinkedList<>();
    // 同一个bucket内数据的partition path和fileId是相同的，这里获取第一条record的信息就可以
    final HoodieRecord<?> record = inputRecords.get(0);
    final String partitionPath = record.getPartitionPath();
    final String fileId = record.getCurrentLocation().getFileId();
    // 指定bucket类型
    final BucketType bucketType = record.getCurrentLocation().getInstantTime().equals("I")
        ? BucketType.INSERT
        : BucketType.UPDATE;
    // 处理upsert分区逻辑
    handleUpsertPartition(
        instantTime,
        partitionPath,
        fileId,
        bucketType,
        inputRecords.iterator())
        .forEachRemaining(writeStatuses::addAll);
    // 设置写入状态元数据
    setUpWriteMetadata(writeStatuses, result);
    return result;
  }

  protected void setUpWriteMetadata(
      List<WriteStatus> statuses,
      HoodieWriteMetadata<List<WriteStatus>> result) {
    // No need to update the index because the update happens before the write.
    result.setWriteStatuses(statuses);
    result.setIndexUpdateDuration(Duration.ZERO);
  }

  @Override
  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  @Override
  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<List<WriteStatus>> result) {
    commit(extraMetadata, result, result.getWriteStatuses().stream().map(WriteStatus::getStat).collect(Collectors.toList()));
  }

  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<List<WriteStatus>> result, List<HoodieWriteStat> writeStats) {
    String actionType = getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    result.setCommitted(true);
    result.setWriteStats(writeStats);
    // Finalize write
    finalizeWrite(instantTime, writeStats, result);
    syncTableMetadata();
    try {
      LOG.info("Committing " + instantTime + ", action Type " + getCommitActionType());
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      HoodieCommitMetadata metadata = CommitUtils.buildMetadata(writeStats, result.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());

      activeTimeline.saveAsComplete(new HoodieInstant(true, getCommitActionType(), instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
      result.setCommitMetadata(Option.of(metadata));
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(List<WriteStatus> writeStatuses) {
    return Collections.emptyMap();
  }

  @Override
  protected boolean isWorkloadProfileNeeded() {
    return true;
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(
      String instantTime,
      String partitionPath,
      String fileIdHint,
      BucketType bucketType,
      Iterator recordItr) {
    try {
      // 根据bucketType种类的不同，执行不同的处理操作
      switch (bucketType) {
        case INSERT:
          return handleInsert(fileIdHint, recordItr);
        case UPDATE:
          // 根据handle种类的不同，执行不同的处理操作
          if (this.writeHandle instanceof HoodieCreateHandle) {
            // During one checkpoint interval, an insert record could also be updated,
            // for example, for an operation sequence of a record:
            //    I, U,   | U, U
            // - batch1 - | - batch2 -
            // the first batch(batch1) operation triggers an INSERT bucket,
            // the second batch batch2 tries to reuse the same bucket
            // and append instead of UPDATE.
            return handleInsert(fileIdHint, recordItr);
          }
          return handleUpdate(partitionPath, fileIdHint, recordItr);
        default:
          throw new HoodieUpsertException("Unknown bucketType " + bucketType + " for partition :" + partitionPath);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + bucketType + " for partition :" + partitionPath;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(recordItr);
    return handleUpdateInternal(upsertHandle, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle<?, ?, ?, ?> upsertHandle, String fileId)
      throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      FlinkMergeHelper.newInstance().runMerge(table, upsertHandle);
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.writeStatuses());
    }

    return Collections.singletonList(upsertHandle.writeStatuses()).iterator();
  }

  protected FlinkMergeHandle getUpdateHandle(Iterator<HoodieRecord<T>> recordItr) {
    if (table.requireSortedRecords()) {
      throw new HoodieNotSupportedException("Sort records are not supported in Flink streaming write");
    } else {
      FlinkMergeHandle writeHandle = (FlinkMergeHandle) this.writeHandle;
      // add the incremental records.
      if (!writeHandle.isNeedBootStrap()) {
        writeHandle.rollOver(recordItr);
      }
      return writeHandle;
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    // 如果record无数据，返回WriteStatus空集合对应的迭代器
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // 否则返回一个FlinkLazyInsertIterable
    // `FlinkLazyInsertIterable`是一个延时的迭代器，
    // 也就是说，只有在遍历`writeStatus`的时候，才会执行数据的插入操作。
    // 我们看下它是怎么实现的。它的`next`方法位于父类`LazyIterableIterator`中
    return new FlinkLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        taskContextSupplier, new ExplicitWriteHandleFactory<>(writeHandle));
  }
}
