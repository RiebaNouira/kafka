const express = require('express');
const { pool } = require('./db');

const app = express();
const PORT = 3000;

app.use(express.json());

// 1. GET /messages : Récupérer tous les messages
app.get('/messages', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM kafka_messages ORDER BY inserted_at DESC');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur lors de la récupération des messages.' });
  }
});

// 2. GET /messages/:id : Récupérer un message spécifique par son ID
app.get('/messages/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query('SELECT * FROM kafka_messages WHERE id = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Message non trouvé.' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur lors de la récupération du message.' });
  }
});

app.listen(PORT, () => {
  console.log(`[API REST] Serveur Express démarré sur http://localhost:${PORT}`);
});