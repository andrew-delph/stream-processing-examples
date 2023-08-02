// consumer.js
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-consumer",
  brokers: ["kafka:9092"],
});

const topic = "my-topic";
const consumer = kafka.consumer({ groupId: "test-group" });

const consumeMessage = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`Received message: ${message.value.toString()}`);
    },
  });
};

consumeMessage().catch(console.error);
