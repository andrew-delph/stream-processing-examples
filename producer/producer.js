// producer.js
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["kafka:9092"],
});

function generateRandomString(length) {
  //   const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const characters = "ABCDEFG";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}

const topic = "my-topic";
const producer = kafka.producer();

const produceMessage = async () => {
  await producer.connect();
  setInterval(async () => {
    const message = { value: generateRandomString(1) };
    await producer.send({ topic, messages: [message] });
    console.log("Message sent", message);
  }, 10);
};

produceMessage().catch(console.error);
