const { Kafka } = require('kafkajs');
const { Worker, isMainThread, parentPort, threadId } = require('worker_threads');
const tweetSchema = require('./schema/tweetSchema'); // Ensure correct path
const os = require('os');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// InfluxDB configuration
const INFLUX_URL = 'http://localhost:8086';
const INFLUX_TOKEN = 'OS0t8w6jBnwwL-HIWgU1lWniUARhRc85gLtFqTbhZiEqVNPvludyzs1vswBDAsegbfWk1pJGpk3dY1LKK_2zDQ==';
const INFLUX_ORG = '6914ea40681fbe1d';
const INFLUX_BUCKET = 'brandpulse';

// Kafka configuration
const kafkaConfig = {
  clientId: `brandpulse-consumer-${process.pid}-${threadId}`,
  brokers: ['localhost:9092'],
  retry: {
    retries: 5,
    initialRetryTime: 100,
    maxRetryTime: 3000
  }
};

const CONSUMER_GROUP = 'brandpulse-consumer-group';
const TOPIC = 'tweets';
// Reduced batch size for more frequent writes
const INFLUX_BATCH_SIZE = 5000;
// More frequent flush interval
const FLUSH_INTERVAL_MS = 5000;

// Worker Logic
if (!isMainThread) {
  const consumer = new Kafka(kafkaConfig).consumer({
    groupId: CONSUMER_GROUP,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 20 * 1024 * 1024,
    maxBytes: 200 * 1024 * 1024,
    maxPollInterval: 300000
  });

  const influxClient = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
  const writeApi = influxClient.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 'ns', {
    // Add write options for better debugging
    defaultTags: { source: 'kafkaConsumer' },
    writeOptions: {
      batchSize: INFLUX_BATCH_SIZE,
      flushInterval: FLUSH_INTERVAL_MS,
      maxRetries: 5,
      maxRetryDelay: 15000,
      minRetryDelay: 1000,
      retryJitter: 1000
    }
  });
  
  let pointBuffer = [];
  let totalFlushedPoints = 0;
  
  const flushPointsToInflux = async () => {
    if (pointBuffer.length === 0) {
      parentPort.postMessage('No points to flush');
      return;
    }
    
    try {
      parentPort.postMessage(`Attempting to flush ${pointBuffer.length} points to InfluxDB`);
      const startTime = Date.now();
      
      // Write points to InfluxDB
      writeApi.writePoints(pointBuffer);
      await writeApi.flush();
      
      const flushDuration = Date.now() - startTime;
      totalFlushedPoints += pointBuffer.length;
      
      parentPort.postMessage({
        type: 'influxFlush',
        message: `Successfully flushed ${pointBuffer.length} points to InfluxDB in ${flushDuration}ms`,
        totalFlushed: totalFlushedPoints
      });
      
      pointBuffer = [];
    } catch (error) {
      parentPort.postMessage({
        type: 'error',
        message: `InfluxDB write error: ${error.message}`,
        stack: error.stack
      });
      
      // Don't clear buffer on error - we'll retry next time
      if (pointBuffer.length > INFLUX_BATCH_SIZE * 3) {
        parentPort.postMessage('Buffer too large after errors, discarding oldest points');
        // Discard oldest half of points to prevent memory issues
        pointBuffer = pointBuffer.slice(Math.floor(pointBuffer.length / 2));
      }
    }
  };

  const runConsumer = async () => {
    try {
      await consumer.connect();
      parentPort.postMessage({ type: 'status', status: 'Kafka connected' });
      
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      parentPort.postMessage(`Subscribed to topic ${TOPIC}`);
      
      await consumer.run({
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 100,
        eachBatchAutoResolve: true,
        eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
          const { topic, partition, messages } = batch;
          
          parentPort.postMessage(`Received batch from partition ${partition} with ${messages.length} messages`);
          
          if (messages.length === 0) {
            parentPort.postMessage(`No messages in batch from partition ${partition}`);
            return;
          }
          
          const startTime = Date.now();
          let processedCount = 0;
          let errorCount = 0;
          
          for (const message of messages) {
            try {
              const decodedValue = tweetSchema.fromBuffer(message.value);
              
              // Create a proper InfluxDB point - FIXED TIMESTAMP HANDLING
              // Use nanosecond precision for InfluxDB
              const timestamp = new Date(decodedValue.timestamp);
              // Add microsecond variation to prevent overwrites
              timestamp.setMilliseconds(timestamp.getMilliseconds() + Math.random());
              
              const point = new Point('tweets')
                .tag('brand', 'SuperCoffee')
                .tag('sentiment', decodedValue.sentiment)
                .stringField('text', decodedValue.text.substring(0, 255))
                .intField('count', 1)
                .timestamp(timestamp);
              
              pointBuffer.push(point);
              processedCount++;
              
              resolveOffset(message.offset);
            } catch (err) {
              errorCount++;
              parentPort.postMessage(`Error processing message: ${err.message}`);
              // Don't fail the entire batch for one bad message
            }
            
            if (processedCount % 500 === 0) {
              await heartbeat();
            }
          }
          
          if (pointBuffer.length >= INFLUX_BATCH_SIZE) {
            await flushPointsToInflux();
          }
          
          const duration = Date.now() - startTime;
          parentPort.postMessage({
            type: 'batchProcessed',
            message: `Processed ${processedCount} messages (${errorCount} errors) in ${duration}ms from partition ${partition}`,
            bufferedPoints: pointBuffer.length
          });
        }
      });
    } catch (err) {
      parentPort.postMessage({
        type: 'error',
        message: `Consumer startup error: ${err.message}`,
        stack: err.stack
      });
    }
  };

  // More frequent flush interval
  const flushInterval = setInterval(flushPointsToInflux, FLUSH_INTERVAL_MS);

  // Add health check interval
  const healthCheckInterval = setInterval(() => {
    parentPort.postMessage({
      type: 'healthCheck',
      bufferedPoints: pointBuffer.length,
      totalFlushedPoints: totalFlushedPoints
    });
  }, 30000);

  process.on('SIGTERM', async () => {
    clearInterval(flushInterval);
    clearInterval(healthCheckInterval);
    parentPort.postMessage('Received SIGTERM, flushing remaining points...');
    await flushPointsToInflux();
    await writeApi.close();
    await consumer.disconnect();
    parentPort.postMessage('Worker shutdown complete');
    process.exit(0);
  });

  runConsumer().catch(err => {
    parentPort.postMessage({
      type: 'fatal',
      message: `Fatal consumer error: ${err.message}`,
      stack: err.stack
    });
    process.exit(1);
  });
}

// Main Thread
if (isMainThread) {
  const WORKER_COUNT = Math.min(os.cpus().length, 2);
  const workers = new Set();

  console.log(`Main consumer process started. Spawning ${WORKER_COUNT} workers`);

  const spawnWorker = (id) => {
    const worker = new Worker(__filename);
    worker
      .on('message', (msg) => {
        // Better message handling
        if (typeof msg === 'object' && msg.type) {
          if (msg.type === 'error' || msg.type === 'fatal') {
            console.error(`[Consumer-W${id}] ${msg.message}`);
            if (msg.stack) console.error(`[Consumer-W${id}] Stack: ${msg.stack}`);
          } else {
            console.log(`[Consumer-W${id}] ${msg.type}: ${msg.message || JSON.stringify(msg)}`);
          }
        } else {
          console.log(`[Consumer-W${id}] ${typeof msg === 'object' ? JSON.stringify(msg) : msg}`);
        }
      })
      .on('error', (err) => console.error(`[Consumer-W${id}] Error: ${err.message}`))
      .on('exit', (code) => {
        console.log(`[Consumer-W${id}] Exited with code ${code}`);
        workers.delete(worker);
        if (code !== 0) spawnWorker(id);
      });
    workers.add(worker);
  };

  for (let i = 0; i < WORKER_COUNT; i++) {
    spawnWorker(i + 1);
  }

  process.on('SIGINT', async () => {
    console.log('\nGracefully shutting down consumer...');
    for (const worker of workers) {
      worker.postMessage({ type: 'shutdown' });
      setTimeout(() => worker.terminate(), 15000); // Force terminate after 15s if needed
    }
    setTimeout(() => process.exit(0), 20000); // Ensure we exit eventually
  });
}