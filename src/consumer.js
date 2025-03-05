const { Kafka } = require('kafkajs');
const { Worker, isMainThread, parentPort, threadId } = require('worker_threads');
const tweetSchema = require('./schema/tweetSchema'); // Ensure path is correct
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

// Consumer group and topic
const CONSUMER_GROUP = 'brandpulse-consumer-group';
const TOPIC = 'tweets'; // Matches producer topic

// Batch size for InfluxDB writes
const INFLUX_BATCH_SIZE = 10000;

// Worker Logic
if (!isMainThread) {
  const consumer = new Kafka(kafkaConfig).consumer({
    groupId: CONSUMER_GROUP,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 20 * 1024 * 1024, // 20MB per partition
    maxBytes: 200 * 1024 * 1024, // 200MB total fetch size
    maxPollInterval: 300000 // Prevent consumer timeout
  });

  // InfluxDB client
  const influxClient = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
  const writeApi = influxClient.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 'ns');
  
  let pointBuffer = [];
  
  const flushPointsToInflux = async () => {
    if (pointBuffer.length === 0) return;
    
    try {
      await writeApi.writePoints(pointBuffer);
      await writeApi.flush();
      parentPort.postMessage(`Flushed ${pointBuffer.length} points to InfluxDB`);
      pointBuffer = [];
    } catch (error) {
      parentPort.postMessage(`InfluxDB write error: ${error.message}`);
      // Keep buffer for retry
    }
  };

  const runConsumer = async () => {
    await consumer.connect();
    parentPort.postMessage({ type: 'status', status: 'connected' });
    
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true }); // Start from beginning for testing
    
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 5000,
      autoCommitThreshold: 100,
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        const { topic, partition, messages } = batch;
        
        if (messages.length === 0) {
          parentPort.postMessage(`No messages in batch from partition ${partition}`);
          return;
        }
        
        const startTime = Date.now();
        let processedCount = 0;
        
        for (const message of messages) {
          try {
            const decodedValue = tweetSchema.fromBuffer(message.value);
            console.log("decodedValue", decodedValue)
            
            const point = new Point('tweets')
              .tag('sentiment', decodedValue.sentiment)
              .timestamp(decodedValue.timestamp)
            //   .timestamp(decodedValue.timestamp * 1000000); // Convert ms to ns
            
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
  };

  // Periodic flush
  const flushInterval = setInterval(flushPointsToInflux, 10000);

  process.on('SIGTERM', async () => {
    clearInterval(flushInterval);
    await flushPointsToInflux();
    await writeApi.close();
    await consumer.disconnect();
    process.exit(0);
  });

  runConsumer().catch(err => {
    parentPort.postMessage(`Fatal consumer error: ${err.message}`);
    process.exit(1);
  });
}

// Main Thread
if (isMainThread) {
  const WORKER_COUNT = Math.min(os.cpus().length, 8);
  
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