// producer.js
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["kafka:9092"],
});

const topic = "my-topic";
const producer = kafka.producer();

const produceMessage = async () => {
  await producer.connect();
  setInterval(async () => {
    const message = { value: "Hello Kafka!" };
    await producer.send({ topic, messages: [message] });
    console.log("Message sent", message);
  }, 1000);
};

produceMessage().catch(console.error);
