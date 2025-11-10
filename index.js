const { Kafka } = require('kafkajs');
const express = require('express');

const kafkaBrokers = process.env.KAFKA_BROKERS?.split(',') || ['kafka.kafka.svc.cluster.local:9092'];

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: kafkaBrokers,
  ssl: false
});

const consumer = kafka.consumer({ groupId: process.env.CONSUMER_GROUP || 'consumer-group' });
console.log(kafkaBrokers)

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC || 'events', fromBeginning: true });
  console.log('✅ Kafka consumer connected and subscribed.');

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

  const app = express();
  app.get('/healthz', (req, res) => res.send('ok'));
  app.listen(process.env.PORT || 3001, () => console.log('Consumer running on 3001'));
}

start().catch(err => {
  console.error('❌ Consumer error:', err);
  process.exit(1);
});
