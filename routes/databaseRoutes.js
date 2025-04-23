const express = require('express');
const router = express.Router();
const databaseController = require('../controllers/databaseController');

// Connection routes
router.post('/connect/retool', databaseController.connectRetool);
router.post('/connect/supabase', databaseController.connectSupabase);

// Table listing routes
router.get('/tables/retool', databaseController.getRetoolTables);
router.get('/tables/supabase', databaseController.getSupabaseTables);

// Table preview route
router.get('/preview/:dbType/:tableName', databaseController.getTablePreview);

// Migration route
router.post('/migrate', databaseController.migrateData);

module.exports = router; 