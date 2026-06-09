const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'kafka_db',
  password: 'password',
  port: 5432,
});

// La fonction qui manquait ou qui n'était pas exportée proprement
const initDb = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS kafka_messages (
      id SERIAL PRIMARY KEY,
      topic VARCHAR(255) NOT NULL,
      partition INT NOT NULL,
      "offset" VARCHAR(50) NOT NULL,
      key TEXT,
      payload JSONB,
      inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `;
  try {
    await pool.query(createTableQuery);
    console.log("Structure PostgreSQL prête (table kafka_messages vérifiée).");
  } catch (error) {
    console.error("Erreur d'initialisation de la table PostgreSQL:", error);
  }
};

// ATTENTION ICI : Il faut absolument exporter les deux dans un objet !
module.exports = { pool, initDb };