const { Pool } = require('pg');
const mysql = require('mysql2/promise');

class DatabaseConfig {
    constructor() {
        this.retoolPool = null;
        this.supabasePool = null;
        this.migrationProgress = {
            totalRecords: 0,
            migratedRecords: 0,
            percentage: 0,
            isComplete: false
        };
    }

    resetMigrationProgress() {
        this.migrationProgress = {
            totalRecords: 0,
            migratedRecords: 0,
            percentage: 0,
            isComplete: false
        };
    }

    getMigrationProgress() {
        return this.migrationProgress;
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
            // Parse the connection string
            const url = new URL(connectionString);
            console.log('Attempting to connect to Supabase at host:', url.hostname);
            
            // Configure the pool with specific settings
            const poolConfig = {
                connectionString: connectionString,
                ssl: {
                    rejectUnauthorized: false,
                    sslmode: 'require'
                },
                // Add specific timeouts and connection settings
                connectionTimeoutMillis: 20000, // Increased timeout
                query_timeout: 20000,
                statement_timeout: 20000,
                idle_in_transaction_session_timeout: 20000,
                max: 20, // Maximum number of clients in the pool
                keepAlive: true,
                // Try both IPv4 and IPv6
                family: 0
            };

            // console.log('Creating connection pool with config:', { ...poolConfig, connectionString: '***hidden***' });
            this.supabasePool = new Pool(poolConfig);

            // Add error handler to the pool
            this.supabasePool.on('error', (err) => {
                console.error('Unexpected error on idle client:', err);
            });

            // Test the connection with a timeout
            console.log('Testing connection...');
            const connectPromise = this.supabasePool.query('SELECT NOW()');
            const timeoutPromise = new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Connection timed out after 20 seconds')), 20000)
            );

            const result = await Promise.race([connectPromise, timeoutPromise]);
            console.log('Connection test successful:', result.rows[0]);
            
            return { success: true, message: 'Successfully connected to Supabase database' };
        } catch (error) {
            console.error('Detailed Supabase connection error:', {
                errorCode: error.code,
                errorMessage: error.message,
                errorStack: error.stack,
                errorDetail: error.detail
            });
            
            // Clean up failed connection
            if (this.supabasePool) {
                console.log('Cleaning up failed connection pool...');
                await this.supabasePool.end().catch(console.error);
                this.supabasePool = null;
            }

            // Provide more specific error messages
            if (error.code === 'ENETUNREACH') {
                return { 
                    success: false, 
                    message: `Unable to reach Supabase database at ${url.hostname}. Please check if your deployment environment allows outbound connections to port ${url.port || 5432}.` 
                };
            } else if (error.code === 'ETIMEDOUT') {
                return { 
                    success: false, 
                    message: `Connection to Supabase timed out at ${url.hostname}. This might be due to firewall rules or network restrictions.` 
                };
            } else if (error.code === 'ENOTFOUND') {
                return { 
                    success: false, 
                    message: `Could not resolve Supabase host: ${url.hostname}. Please check your connection string and DNS settings.` 
                };
            } else if (error.code === '28P01') {
                return {
                    success: false,
                    message: 'Invalid credentials. Please check your username and password.'
                };
            } else if (error.code === '3D000') {
                return {
                    success: false,
                    message: 'Database does not exist. Please check your connection string.'
                };
            }

            return { 
                success: false, 
                message: `Failed to connect to Supabase database: ${error.message}. Please check your connection string and ensure the database is accessible.` 
            };
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
            const result = await this.retoolPool.query(`SELECT * FROM ${sourceTable} LIMIT 5000`);
            sourceData = result.rows;
            console.log(`Fetched ${sourceData.length} records from ${sourceTable}`);
        } else {
            const [rows] = await this.retoolPool.query(`SELECT * FROM ${sourceTable} LIMIT 5000`);
            sourceData = rows;
            console.log(`Fetched ${sourceData.length} records from ${sourceTable}`);
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
        
        this.migrationProgress.totalRecords = totalCount;
        console.log(`Starting migration of ${sourceData.length} records out of total ${totalCount}`);
        
        // Insert into target
        let migratedCount = 0;
        const startTime = Date.now();
        const logInterval = setInterval(() => {
            const elapsedSeconds = Math.floor((Date.now() - startTime) / 1000);
            const percentage = Math.round((migratedCount / totalCount) * 100);
            console.log(`Migration progress: ${migratedCount} records migrated in ${elapsedSeconds} seconds (${percentage}%)`);
        }, 30000); // Log every 30 seconds

        try {
            for (const row of sourceData) {
                const columns = Object.keys(row);
                const values = Object.values(row);
                const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
                
                await this.supabasePool.query(
                    `INSERT INTO ${targetTable} (${columns.join(', ')}) VALUES (${placeholders})`,
                    values
                );
                migratedCount++;
                this.migrationProgress.migratedRecords = migratedCount;
                this.migrationProgress.percentage = Math.round((migratedCount / totalCount) * 100);
            }
        } finally {
            clearInterval(logInterval);
            this.migrationProgress.isComplete = true;
        }

        const totalTime = Math.floor((Date.now() - startTime) / 1000);
        console.log(`Migration completed: ${migratedCount} records migrated in ${totalTime} seconds`);

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