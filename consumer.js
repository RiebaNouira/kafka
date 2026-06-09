const { Kafka } = require('kafkajs');
const { pool, initDb } = require('./db');

const kafka = new Kafka({
  clientId: 'tp6-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'tp6-group' });
const topic = process.env.KAFKA_TOPIC || 'test-topic';

const run = async () => {
  // 1. Initialiser la base de données
  await initDb();

  // 2. Connecter et s'abonner au topic Kafka
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  console.log(`[Consommateur] Connecté et abonné au topic : ${topic}`);

  // 3. Traiter chaque message entrant
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key ? message.key.toString() : null;
      const rawValue = message.value ? message.value.toString() : null;
      
      let payload = null;
      try {
        payload = JSON.parse(rawValue);
      } catch (e) {
        payload = { raw: rawValue }; // Fallback si ce n'est pas du JSON valide
      }

      const offset = message.offset;

      try {
        const insertQuery = `
          INSERT INTO kafka_messages (topic, partition, "offset", key, payload)
          VALUES ($1, $2, $3, $4, $5)
        `;
        await pool.query(insertQuery, [topic, partition, offset, key, JSON.stringify(payload)]);
        console.log(`[Consommateur] Message stocké en BDD -> Offset: ${offset}, Key: ${key}`);
      } catch (err) {
        console.error("[Consommateur] Erreur lors de l'enregistrement en BDD:", err);
      }
    },
  });
};

run().catch(console.error);