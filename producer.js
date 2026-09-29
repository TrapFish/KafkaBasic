const { kafka, TOPIC } = require('./kafka');

const customers = ['alice', 'bob', 'carol', 'dave'];
const items = ['coffee', 'tea', 'cake', 'cookie'];
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

async function main() {
  // Create the topic (3 partitions) if it doesn't exist yet
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
  });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  console.log('Producer connected. Sending an order every second (Ctrl+C to stop)');

  let id = 1;
  const timer = setInterval(async () => {
    const order = {
      orderId: id++,
      customer: pick(customers),
      item: pick(items),
      qty: 1 + Math.floor(Math.random() * 3),
      createdAt: new Date().toISOString(),
    };

    // Same key => same partition => per-customer ordering is preserved
    const [meta] = await producer.send({
      topic: TOPIC,
      messages: [{ key: order.customer, value: JSON.stringify(order) }],
    });
    console.log("Order in producer ::", order);
    console.log(`Sent order #${order.orderId} (${order.customer}) -> partition ${meta.partition}`);
  }, 1000);

  const shutdown = async () => {
    clearInterval(timer);
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
