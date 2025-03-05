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
const INFLUX_BATCH_SIZE = 10000;

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
  const writeApi = influxClient.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 'ns');
  
  let pointBuffer = [];
  
  const flushPointsToInflux = async () => {
    if (pointBuffer.length === 0) {
      parentPort.postMessage('No points to flush');
      return;
    }
    
    try {
      parentPort.postMessage(`Attempting to flush ${pointBuffer.length} points`);
      await writeApi.writePoints(pointBuffer);
      await writeApi.flush();
      parentPort.postMessage(`Successfully flushed ${pointBuffer.length} points to InfluxDB`);
      pointBuffer = [];
    } catch (error) {
      parentPort.postMessage(`InfluxDB write error: ${error.message}`);
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
          
          for (const message of messages) {
            try {
              const decodedValue = tweetSchema.fromBuffer(message.value);
              
              const point = new Point('tweets')
                .tag('sentiment', decodedValue.sentiment)
                .intField('count', 1) // Add count field for aggregation
                .timestamp(decodedValue.timestamp * 1000000);
              
              pointBuffer.push(point);
              processedCount++;
              
              resolveOffset(message.offset);
            } catch (err) {
              parentPort.postMessage(`Error decoding message: ${err.message}`);
            }
            
            if (processedCount % 500 === 0) {
              await heartbeat();
            }
          }
          
          if (pointBuffer.length >= INFLUX_BATCH_SIZE) {
            await flushPointsToInflux();
          }
          
          const duration = Date.now() - startTime;
          parentPort.postMessage(`Processed ${processedCount} messages in ${duration}ms from partition ${partition}`);
        }
      });
    } catch (err) {
      parentPort.postMessage(`Consumer startup error: ${err.message}`);
    }
  };

  const flushInterval = setInterval(flushPointsToInflux, 10000);

  process.on('SIGTERM', async () => {
    clearInterval(flushInterval);
    await flushPointsToInflux();
    await writeApi.close();
    await consumer.disconnect();
    parentPort.postMessage('Worker shutdown complete');
    process.exit(0);
  });

  runConsumer().catch(err => {
    parentPort.postMessage(`Fatal consumer error: ${err.message}`);
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
      .on('message', (msg) => console.log(`[Consumer-W${id}] ${typeof msg === 'object' ? JSON.stringify(msg) : msg}`))
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
      await worker.terminate();
    }
    process.exit(0);
  });
}