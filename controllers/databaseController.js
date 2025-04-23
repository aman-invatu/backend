const databaseConfig = require('../config/database');

class DatabaseController {
    async connectRetool(req, res) {
        try {
            const { connectionString } = req.body;
            if (!connectionString) {
                return res.status(400).json({ success: false, message: 'Connection string is required' });
            }

            const result = await databaseConfig.connectRetool(connectionString);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }

    async connectSupabase(req, res) {
        try {
            const { connectionString } = req.body;
            if (!connectionString) {
                return res.status(400).json({ success: false, message: 'Connection string is required' });
            }

            const result = await databaseConfig.connectSupabase(connectionString);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }

    async getRetoolTables(req, res) {
        try {
            const tables = await databaseConfig.getRetoolTables();
            res.json({ success: true, tables });
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }

    async getSupabaseTables(req, res) {
        try {
            const tables = await databaseConfig.getSupabaseTables();
            res.json({ success: true, tables });
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }

    async getTablePreview(req, res) {
        try {
            const { dbType, tableName } = req.params;
            if (!dbType || !tableName) {
                return res.status(400).json({ success: false, message: 'Database type and table name are required' });
            }

            const data = await databaseConfig.getTablePreview(dbType, tableName);
            res.json({ success: true, data });
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }

    async migrateData(req, res) {
        try {
            const { sourceTable, targetTable } = req.body;
            if (!sourceTable || !targetTable) {
                return res.status(400).json({ success: false, message: 'Source and target table names are required' });
            }

            const result = await databaseConfig.migrateData(sourceTable, targetTable);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, message: error.message });
        }
    }
}

module.exports = new DatabaseController(); 