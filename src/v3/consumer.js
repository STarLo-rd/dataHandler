const { Kafka } = require('kafkajs');
const { Worker, isMainThread, parentPort, threadId } = require('worker_threads');
const tweetSchema = require('../schema/tweetSchema');
const os = require('os');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');
const { Queue, Worker: BullWorker } = require('bullmq');

// System configuration
const totalMemory = os.totalmem();
const MAX_MEMORY_USAGE = totalMemory * 0.85; // Max 90% of system memory

// InfluxDB configuration
const INFLUX_URL = 'http://localhost:8086';
const INFLUX_TOKEN = 'OS0t8w6jBnwwL-HIWgU1lWniUARhRc85gLtFqTbhZiEqVNPvludyzs1vswBDAsegbfWk1pJGpk3dY1LKK_2zDQ==';
const INFLUX_ORG = '6914ea40681fbe1d';
const INFLUX_BUCKET = 'brandpulse';

// Kafka configuration
const kafkaConfig = {
  clientId: `brandpulse-consumer-${process.pid}-${threadId || 0}`,
  brokers: ['localhost:9092'],
  retry: { retries: 5, initialRetryTime: 100, maxRetryTime: 2000 },
};

const CONSUMER_GROUP = 'brandpulse-consumer-group';
const TOPIC = 'tweets';
const INFLUX_BATCH_SIZE = 10000; // Larger batch size for throughput
const WORKER_COUNT = Math.min(os.cpus().length, 6); // More consumer workers
const WRITER_COUNT = 4; // More writer workers
const FLUSH_INTERVAL_MS = 500; // Faster flush interval
const REDIS_CONFIG = { host: 'localhost', port: 6379 };

// Memory monitoring function
function checkMemoryUsage() {
  const used = process.memoryUsage().heapUsed;
  const freeMemory = os.freemem();
  const systemUsedMemory = totalMemory - freeMemory;
  
  return {
    processMemory: used,
    systemMemory: systemUsedMemory,
    percentUsed: (systemUsedMemory / totalMemory) * 100,
    overThreshold: systemUsedMemory > MAX_MEMORY_USAGE,
  };
}

// Main Thread
if (isMainThread) {
  console.log(`Starting BrandPulse pipeline with ${WORKER_COUNT} consumer workers and ${WRITER_COUNT} writer workers (High-Throughput with BullMQ)`);
  console.log(`System memory: ${(totalMemory / (1024 * 1024 * 1024)).toFixed(2)} GB`);
  console.log(`Max memory usage: ${(MAX_MEMORY_USAGE / (1024 * 1024 * 1024)).toFixed(2)} GB (90%)`);
  
  const workers = new Map();
  let totalProcessed = 0;
  let systemPaused = false;
  
  const influxQueue = new Queue('influx-writes', { connection: REDIS_CONFIG });
  
  // Spawn consumer workers
  const spawnConsumerWorker = (id) => {
    const worker = new Worker(__filename, { env: { IS_CONSUMER: true } });
    workers.set(id, worker);
    
    worker.on('message', (msg) => {
      if (msg.type === 'processed') {
        totalProcessed += msg.count;
      } else if (msg.type === 'error' || msg.type === 'fatal') {
        console.error(`[ConsumerWorker ${id}] ${msg.message}`);
      } else {
        console.log(`[ConsumerWorker ${id}] ${msg.type}: ${msg.message || JSON.stringify(msg)}`);
      }
    });
    worker.on('error', (err) => console.error(`[ConsumerWorker ${id}] Error: ${err.message}`));
    worker.on('exit', (code) => {
      workers.delete(id);
      if (code !== 0 && !systemPaused) {
        if (!checkMemoryUsage().overThreshold) {
          setTimeout(() => spawnConsumerWorker(id), 5000);
        }
      }
    });
  };
  
  // Spawn writer workers (BullMQ)
  const writerWorkers = [];
  for (let i = 0; i < WRITER_COUNT; i++) {
    const writer = new BullWorker(
      'influx-writes',
      async (job) => {
        const { data } = job.data;
        const startTime = Date.now();
        try {
          const influxClient = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
          const writeApi = influxClient.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 'ns', {
            writeOptions: {
              batchSize: INFLUX_BATCH_SIZE,
              flushInterval: FLUSH_INTERVAL_MS,
              maxRetries: 3,
            },
          });
          
          const points = data.map((item) =>
            new Point('tweets')
              .tag('brand', 'SuperCoffee')
              .tag('sentiment', item.sentiment)
              .stringField('text', item.text)
              .intField('count', 1)
              .timestamp(new Date(item.timestamp))
          );
          
          writeApi.writePoints(points);
          await writeApi.flush();
          console.log(`[WriterWorker ${i + 1}] Flushed ${points.length} points in ${Date.now() - startTime}ms`);
          await writeApi.close();
        } catch (err) {
          console.error(`[WriterWorker ${i + 1}] Write error: ${err.message}`);
          throw err; // Retry job
        }
      },
      { connection: REDIS_CONFIG, concurrency: 2 } // Higher concurrency for throughput
    );
    writerWorkers.push(writer);
  }
  
  // Start consumer workers gradually
  for (let i = 1; i <= WORKER_COUNT; i++) {
    setTimeout(() => spawnConsumerWorker(i), i * 1000); // Reduced delay for faster startup
  }
  
  // System-wide memory monitoring
  const monitorMemory = () => {
    const memUsage = checkMemoryUsage();
    if (memUsage.percentUsed > 85 && !systemPaused) {
      console.log(`⚠️ System memory usage critical: ${memUsage.percentUsed.toFixed(1)}%`);
      for (const [id, worker] of workers) {
        worker.postMessage({ type: 'pause' });
      }
      systemPaused = true;
    } else if (memUsage.percentUsed < 75 && systemPaused) {
      console.log(`✓ System memory usage normal: ${memUsage.percentUsed.toFixed(1)}%`);
      for (const [id, worker] of workers) {
        worker.postMessage({ type: 'resume' });
      }
      systemPaused = false;
    }
    if (Date.now() % 5000 < 500) { // Every 5s for tighter monitoring
      console.log(`Memory usage: ${memUsage.percentUsed.toFixed(1)}% | Free: ${(os.freemem() / (1024 * 1024 * 1024)).toFixed(2)} GB`);
    }
  };
  setInterval(monitorMemory, 500);
  
  // Report stats
  const reportStats = () => {
    console.log(`--- STATS REPORT ---`);
    console.log(`Total processed: ${totalProcessed.toLocaleString()} messages`);
    console.log(`Throughput: ${((totalProcessed / (Date.now() / 1000)) | 0).toLocaleString()} msg/s`);
    console.log(`Active consumer workers: ${workers.size}`);
    console.log(`Active writer workers: ${WRITER_COUNT}`);
    console.log(`Memory: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)} MB (process)`);
    console.log(`System memory: ${((totalMemory - os.freemem()) / totalMemory * 100).toFixed(1)}% used`);
    console.log(`-------------------`);
  };
  setInterval(reportStats, 5000);
  
  // Handle shutdown
  process.on('SIGINT', async () => {
    console.log('\nShutting down...');
    for (const [id, worker] of workers) {
      worker.postMessage({ type: 'shutdown' });
    }
    for (const writer of writerWorkers) {
      await writer.close();
    }
    await influxQueue.close();
    setTimeout(() => process.exit(0), 15000);
  });
}

// Consumer Worker Logic
if (!isMainThread && process.env.IS_CONSUMER) {
  const consumer = new Kafka(kafkaConfig).consumer({
    groupId: `${CONSUMER_GROUP}-${threadId}`,
    sessionTimeout: 30000,
    heartbeatInterval: 5000,
    maxBytesPerPartition: 5 * 1024 * 1024, // Increased to 5MB for more messages
    maxBytes: 20 * 1024 * 1024, // Increased to 20MB
    maxWaitTimeInMs: 50, // Reduced wait time for faster polling
  });
  
  const influxQueue = new Queue('influx-writes', { connection: REDIS_CONFIG });
  
  let dataBuffer = [];
  let totalProcessed = 0;
  let paused = false;
  
  const memoryCheckInterval = setInterval(() => {
    const memUsage = checkMemoryUsage();
    if (memUsage.overThreshold && !paused) {
      paused = true;
      consumer.pause([{ topic: TOPIC }]);
      parentPort.postMessage({ type: 'warning', message: `Memory usage high (${memUsage.percentUsed.toFixed(1)}%). Pausing consumer.` });
      flushToQueue();
    } else if (!memUsage.overThreshold && paused) {
      paused = false;
      consumer.resume([{ topic: TOPIC }]);
      parentPort.postMessage({ type: 'info', message: `Memory usage normal (${memUsage.percentUsed.toFixed(1)}%). Resuming consumer.` });
    }
  }, 500);
  
  const processMessages = async (messages) => {
    const rawData = [];
    for (const message of messages) {
      try {
        const decodedValue = tweetSchema.fromBuffer(message.value);
        rawData.push({
          sentiment: decodedValue.sentiment,
          text: decodedValue.text.substring(0, 100),
          timestamp: Date.now(),
        });
      } catch (err) {
        parentPort.postMessage({ type: 'error', message: `Parse error: ${err.message}` });
      }
    }
    return rawData;
  };
  
  const flushToQueue = async () => {
    if (dataBuffer.length === 0) return;
    try {
      await influxQueue.add('write', { data: dataBuffer });
      parentPort.postMessage({ type: 'queued', message: `Queued ${dataBuffer.length} items to BullMQ` });
      dataBuffer = [];
    } catch (err) {
      parentPort.postMessage({ type: 'error', message: `Queue error: ${err.message}` });
    }
  };
  
  const flushInterval = setInterval(flushToQueue, FLUSH_INTERVAL_MS);
  
  const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
    parentPort.postMessage({ type: 'info', message: `Consumer worker ${threadId} connected` });
    
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 3000,
      autoCommitThreshold: INFLUX_BATCH_SIZE,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        if (paused) return;
        const startTime = Date.now();
        const messages = batch.messages;
        const rawData = await processMessages(messages);
        dataBuffer.push(...rawData);
        totalProcessed += messages.length;
        
        if (dataBuffer.length >= INFLUX_BATCH_SIZE) {
          await flushToQueue();
        }
        
        resolveOffset(messages[messages.length - 1].offset);
        await heartbeat();
        
        parentPort.postMessage({
          type: 'processed',
          count: messages.length,
          message: `Processed ${messages.length} messages in ${Date.now() - startTime}ms`,
        });
      },
    });
  };
  
  runConsumer().catch((err) => {
    parentPort.postMessage({ type: 'fatal', message: `Fatal error: ${err.message}` });
    process.exit(1);
  });
  
  parentPort.on('message', async (msg) => {
    if (msg.type === 'shutdown') {
      clearInterval(flushInterval);
      clearInterval(memoryCheckInterval);
      await flushToQueue();
      await consumer.disconnect();
      await influxQueue.close();
      process.exit(0);
    } else if (msg.type === 'pause') {
      paused = true;
      consumer.pause([{ topic: TOPIC }]);
    } else if (msg.type === 'resume') {
      paused = false;
      consumer.resume([{ topic: TOPIC }]);
    }
  });
}