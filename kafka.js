const { Kafka, logLevel } = require('kafkajs');

const TOPIC = 'orders';

const kafka = new Kafka({
  clientId: 'orders-app',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: logLevel.ERROR,
});

module.exports = { kafka, TOPIC };
