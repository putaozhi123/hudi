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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through a bounded in-memory queue. This
 * class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(BoundedInMemoryExecutor.class);

  // Executor service used for launching writer thread.
  private final ExecutorService executorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final BoundedInMemoryQueue<I, O> queue;
  // Producers
  private final List<BoundedInMemoryQueueProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, BoundedInMemoryQueueProducer<I> producer,
      Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferLimitInBytes, Arrays.asList(producer), consumer, transformFunction, new DefaultSizeEstimator<>());
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, List<BoundedInMemoryQueueProducer<I>> producers,
      Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
      final SizeEstimator<O> sizeEstimator) {
    this.producers = producers;
    this.consumer = consumer;
    // Ensure single thread for each producer thread and one for consumer
    // 确保每个生产者线程有一个线程，而消费者线程有一个线程
    this.executorService = Executors.newFixedThreadPool(producers.size() + 1);
    this.queue = new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator);
  }

  /**
   * Callback to implement environment specific behavior before executors (producers/consumer) run.
   */
  public void preExecute() {
    // Do Nothing in general context
  }

  /**
   * Start all Producers.
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    // Latch to control when and which producer thread will close the queue
    // 创建CountDownLatch，传入参数为producer个数
    // 每个producer运行结束都会countDown
    // 当count数为0的时候表示所有的生产者运行结束，需要关闭内存队列（标记写入结束）
    final CountDownLatch latch = new CountDownLatch(producers.size());
    // 线程池共producers.size() + 1个线程，满足每个producer和唯一的consumer各一个线程
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(executorService);
    producers.stream().map(producer -> {
      // 在新线程中执行生产者逻辑
      return completionService.submit(() -> {
        try {
          // 此方法为空实现
          preExecute();
          // 生产者开始向队列生产数据
          producer.produce(queue);
        } catch (Exception e) {
          LOG.error("error producing records", e);
          // 出现异常，在队列中标记失败
          queue.markAsFailed(e);
          throw e;
        } finally {
          synchronized (latch) {
            // 生产者完成任务或者出现异常的时候，countDown
            latch.countDown();
            // 如果count为0，说明所有生产者任务完成，关闭队列
            if (latch.getCount() == 0) {
              // Mark production as done so that consumer will be able to exit
              queue.close();
            }
          }
        }
        return true;
      });
    }).collect(Collectors.toList());
    return completionService;
  }

  /**
   * Start only consumer.
   */
  private Future<E> startConsumer() {
    return consumer.map(consumer -> {
      return executorService.submit(() -> {
        LOG.info("starting consumer thread");
        // 此方法为空实现
        preExecute();
        try {
          // 消费者开始消费数据 org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume
          E result = consumer.consume(queue);
          LOG.info("Queue Consumption is done; notifying producer threads");
          return result;
        } catch (Exception e) {
          LOG.error("error consuming records", e);
          // 出现异常，在队列中标记失败
          queue.markAsFailed(e);
          throw e;
        }
      });
    }).orElse(CompletableFuture.completedFuture(null));
  }

  /**
   * Main API to run both production and consumption.
   * `BoundedInMemoryExecutor`是一个生产者消费者模型作业的执行器，
   * 生产者结果的缓存使用有界内存队列`BoundedInMemoryQueue`。我们从它的`execute`方法开始分析。
   * `execute`使用线程池，分别启动生产者和消费者作业。然后等待消费者运行完毕，返回结果。
   */
  public E execute() {
    try {
      // 启动生产者
      ExecutorCompletionService<Boolean> producerService = startProducers();
      // 启动消费者
      Future<E> future = startConsumer();
      // Wait for consumer to be done
      // 等待消费者运行完毕，返回结果
      return future.get();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return queue.iterator().hasNext();
  }

  public void shutdownNow() {
    executorService.shutdownNow();
  }

  public BoundedInMemoryQueue<I, O> getQueue() {
    return queue;
  }
}
