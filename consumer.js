const { kafka, TOPIC } = require('./kafka');

async function main() {
  const consumer = kafka.consumer({ groupId: 'order-processor' });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log('Consumer connected. Waiting for orders (Ctrl+C to stop)');

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log("Order in consumer ::", order);
      console.log(
        `[p${partition} @${message.offset}] order #${order.orderId}: ` +
          `${order.customer} wants ${order.qty} x ${order.item}`
      );
    },
  });

  const shutdown = async () => {
    await consumer.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
