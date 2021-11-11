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

package org.apache.hudi.execution;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.List;

public class FlinkLazyInsertIterable<T extends HoodieRecordPayload> extends HoodieLazyInsertIterable<T> {
  public FlinkLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                 boolean areRecordsSorted,
                                 HoodieWriteConfig config,
                                 String instantTime,
                                 HoodieTable hoodieTable,
                                 String idPrefix,
                                 TaskContextSupplier taskContextSupplier) {
    super(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier);
  }

  public FlinkLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                 boolean areRecordsSorted,
                                 HoodieWriteConfig config,
                                 String instantTime,
                                 HoodieTable hoodieTable,
                                 String idPrefix,
                                 TaskContextSupplier taskContextSupplier,
                                 WriteHandleFactory writeHandleFactory) {
    super(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier, writeHandleFactory);
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    BoundedInMemoryExecutor<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> bufferedIteratorExecutor =
        null;
    try {
      // 获取数据的schema
      final Schema schema = new Schema.Parser().parse(hoodieConfig.getSchema());
      // 创建BoundedInMemoryExecutor，是核心内容，后面分析
      // 根据 inputItr 初始化生产者的数量; Option.of(getInsertHandler()) 消费者
      bufferedIteratorExecutor =
          new BoundedInMemoryExecutor<>(hoodieConfig.getWriteBufferLimitBytes(), new IteratorBasedQueueProducer<>(inputItr), Option.of(getInsertHandler()), getTransformFunction(schema));
      // 执行executor。executor的生产者和消费者并发执行，execute方法会等待执行结束后返回
      final List<WriteStatus> result = bufferedIteratorExecutor.execute();
      // 检查结果不为空，并且executor中积压的数据全部处理完毕
      assert result != null && !result.isEmpty() && !bufferedIteratorExecutor.isRemaining();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      // 确保executor使用后关闭
      if (null != bufferedIteratorExecutor) {
        bufferedIteratorExecutor.shutdownNow();
      }
    }
  }
}
