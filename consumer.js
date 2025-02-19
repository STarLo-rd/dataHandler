const { Kafka } = require('kafkajs');
const dataSchema = require("./schema/avroSchema.js")

const kafka = new Kafka({
  clientId: 'dataStorm-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'dataStorm-group' });

const processMessage = async (message) => {
  try {
    // Deserialize Avro buffer
    const data = dataSchema.fromBuffer(message.value);
    console.log(`Parsed message:`, data);
    // Further processing logic...
  } catch (error) {
    console.error(`Failed to parse message:`, error);
  }
};

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'dataStorm-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await processMessage(message);
    },
  });
};

runConsumer().catch(console.error);