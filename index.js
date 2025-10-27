// consumer/index.js
const { Kafka } = require('kafkajs');

const kafkaBrokers = process.env.KAFKA_BROKERS?.split(',') || ['kafka:9092'];
const kafka = new Kafka({ clientId: 'consumer', brokers: kafkaBrokers });
const consumer = kafka.consumer({ groupId: process.env.CONSUMER_GROUP || 'consumer-group' });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC || 'events', fromBeginning: false });
  console.log('Kafka consumer connected, subscribed');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });

  // expose a health endpoint via a tiny server (optional)
  const express = require('express');
  const app = express();
  app.get('/healthz', (req, res) => res.send('ok'));
  app.get('/', (req, res) => res.send('test running'));
  app.listen(process.env.PORT || 3001, () => console.log('Consumer health on 3001'));
}

start().catch(err => { console.error(err); process.exit(1); });
