const { Pool } = require('pg');
const mysql = require('mysql2/promise');

class DatabaseConfig {
    constructor() {
        this.retoolPool = null;
        this.supabasePool = null;
    }

    async connectRetool(connectionString) {
        try {
            // Check if the connection string is for PostgreSQL
            if (connectionString.includes('postgresql://') || connectionString.includes('postgres://')) {
                // Parse the connection string
                const url = new URL(connectionString);
                const config = {
                    user: url.username,
                    password: url.password,
                    host: url.hostname,
                    port: url.port || 5432,
                    database: url.pathname.substring(1),
                    ssl: {
                        rejectUnauthorized: false
                    },
                    connectionTimeoutMillis: 10000,
                    query_timeout: 10000,
                    statement_timeout: 10000,
                    idle_in_transaction_session_timeout: 10000
                };

                this.retoolPool = new Pool(config);
                // Test the connection
                await this.retoolPool.query('SELECT NOW()');
                return { success: true, message: 'Successfully connected to Retool PostgreSQL database' };
            } else {
                // MySQL connection
                this.retoolPool = mysql.createPool({
                    uri: connectionString,
                    connectTimeout: 10000,
                    waitForConnections: true,
                    connectionLimit: 10,
                    queueLimit: 0
                });
                // Test the connection
                await this.retoolPool.getConnection();
                return { success: true, message: 'Successfully connected to Retool database' };
            }
        } catch (error) {
            console.error('Connection error:', error);
            return { success: false, message: `Failed to connect to Retool database: ${error.message}` };
        }
    }

    async connectSupabase(connectionString) {
        try {
            this.supabasePool = new Pool({
                connectionString: connectionString
            });
            // Test the connection
            await this.supabasePool.query('SELECT NOW()');
            return { success: true, message: 'Successfully connected to Supabase database' };
        } catch (error) {
            return { success: false, message: `Failed to connect to Supabase database: ${error.message}` };
        }
    }

    async getRetoolTables() {
        if (!this.retoolPool) {
            throw new Error('Retool database not connected');
        }
        
        // Check if it's a PostgreSQL connection
        if (this.retoolPool instanceof Pool) {
            const result = await this.retoolPool.query(`
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            `);
            return result.rows.map(row => row.table_name);
        } else {
            // MySQL connection
            const [rows] = await this.retoolPool.query('SHOW TABLES');
            return rows.map(row => Object.values(row)[0]);
        }
    }

    async getSupabaseTables() {
        if (!this.supabasePool) {
            throw new Error('Supabase database not connected');
        }
        const result = await this.supabasePool.query(`
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        `);
        return result.rows.map(row => row.table_name);
    }

    async getTablePreview(dbType, tableName, limit = 10) {
        const pool = dbType === 'retool' ? this.retoolPool : this.supabasePool;
        if (!pool) {
            throw new Error(`${dbType} database not connected`);
        }

        // Check if it's a PostgreSQL connection
        if (pool instanceof Pool) {
            const result = await pool.query(`SELECT * FROM ${tableName} LIMIT $1`, [limit]);
            return result.rows;
        } else {
            // MySQL connection
            const [rows] = await pool.query(`SELECT * FROM ${tableName} LIMIT ?`, [limit]);
            return rows;
        }
    }

    async migrateData(sourceTable, targetTable) {
        if (!this.retoolPool || !this.supabasePool) {
            throw new Error('Both databases must be connected for migration');
        }

        // Get data from source with LIMIT 5000
        let sourceData;
        if (this.retoolPool instanceof Pool) {
            // PostgreSQL source
            const result = await this.retoolPool.query(`SELECT * FROM ${sourceTable} LIMIT 5000`);
            sourceData = result.rows;
        } else {
            // MySQL source
            const [rows] = await this.retoolPool.query(`SELECT * FROM ${sourceTable} LIMIT 5000`);
            sourceData = rows;
        }
        
        // Get total count for progress tracking
        let totalCount;
        if (this.retoolPool instanceof Pool) {
            const result = await this.retoolPool.query(`SELECT COUNT(*) as total FROM ${sourceTable}`);
            totalCount = parseInt(result.rows[0].total);
        } else {
            const [rows] = await this.retoolPool.query(`SELECT COUNT(*) as total FROM ${sourceTable}`);
            totalCount = parseInt(rows[0].total);
        }
        
        // Insert into target
        let migratedCount = 0;
        for (const row of sourceData) {
            const columns = Object.keys(row);
            const values = Object.values(row);
            const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
            
            await this.supabasePool.query(
                `INSERT INTO ${targetTable} (${columns.join(', ')}) VALUES (${placeholders})`,
                values
            );
            migratedCount++;
        }

        return { 
            success: true, 
            message: `Data migration completed successfully. Migrated ${migratedCount} out of ${totalCount} records.`,
            migratedCount,
            totalCount,
            hasMoreData: migratedCount < totalCount
        };
    }
}

module.exports = new DatabaseConfig(); 