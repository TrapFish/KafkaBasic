# kafka-orders

A tiny Kafka demo in Node.js using [KafkaJS](https://kafka.js.org).
A producer emits fake coffee-shop orders; a consumer processes them.

## Run it

```bash
docker compose up -d      # start Kafka (single node, KRaft, no ZooKeeper)
npm install

npm run consumer          # terminal 1
npm run producer          # terminal 2
```

## Things to try

- **Scale consumers:** start a second `npm run consumer`. Kafka rebalances the
  3 partitions between them (same `groupId`).
- **Independent readers:** change `groupId` in `consumer.js` and the new group
  reads every message from the start.
- **Ordering:** orders for the same customer always land in the same partition,
  because the message key is the customer name.
- **Replay:** stop the consumer, let the producer run, restart it. It resumes
  from its last committed offset.

## Clean up

```bash
docker compose down
```
# KafkaBasic
