// producer.js
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["kafka:9092"],
});

function generateRandomString(length) {
  const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}

const topic = "my-topic";
const producer = kafka.producer();

let count = 0;
const produceMessage = async () => {
  await producer.connect();
  setInterval(async () => {
    const message = { value: count + "" };
    count = count + 1;
    await producer.send({ topic, messages: [message] });
    console.log("Message sent", message);
  }, 1000);
};

produceMessage().catch(console.error);
