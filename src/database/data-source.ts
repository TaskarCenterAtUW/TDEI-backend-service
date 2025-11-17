import { Pool, PoolClient, Query, QueryConfig, QueryResult, Submittable } from 'pg';
import { PostgresError } from '../constants/pg-error-constants';
import { environment } from '../environment/environment';
import UniqueKeyDbException, { AmbiguousColumnDbException, ForeignKeyDbException, QuerySyntaxErrorDbException, UndefinedColumnDbException, UndefinedFunctionDbException } from '../exceptions/db/database-exceptions';
import QueryStream from 'pg-query-stream';

class DataSource {
    private pool: Pool = new Pool;

    constructor() {
        // TODO document why this constructor is empty

    }

    public initializaDatabase() {
        console.info("Initializing database !");
        this.pool = new Pool({
            database: environment.database.database,
            host: environment.database.host,
            user: environment.database.username,
            password: environment.database.password,
            ssl: environment.database.ssl,
            port: environment.database.port
        });

        this.pool.on('error', function (err: Error) {
            console.log(`Idle-Client Error:\n${err.message}\n${err.stack}`)
        }).on('connect', () => {
            console.log("Database connected successfully !");
        });
    }

    /**
     * Async Query
     * @param sqlText 
     * @param params 
     * @returns 
     */
    async query(queryTextOrConfig: string | QueryConfig<any[]>, params: any[] = []): Promise<QueryResult<any>> {
        const client = await this.pool.connect();
        try {
            if (queryTextOrConfig instanceof String) {
                const result = await client.query(queryTextOrConfig, params);
                return result;
            }
            else {
                const result = await client.query(queryTextOrConfig);
                return result;
            }

        } catch (e: any) {

            switch (e.code) {
                case PostgresError.UNIQUE_VIOLATION:
                    throw new UniqueKeyDbException("Duplicate");
                case PostgresError.FOREIGN_KEY_VIOLATION:
                    throw new ForeignKeyDbException(e.constraint);
                case PostgresError.UNDEFINED_COLUMN:
                    throw new UndefinedColumnDbException(`Undefined column: ${e.message}`);
                case PostgresError.SYNTAX_ERROR:
                    console.error(`Syntax error in SQL: ${e}`);
                    throw new QuerySyntaxErrorDbException(`Query Syntax error`);
                case PostgresError.UNDEFINED_FUNCTION:
                    console.error(`Undefined function in SQL: ${e.message}`);
                    throw new UndefinedFunctionDbException(`Undefined function error : ${e.message}`);
                case PostgresError.AMBIGUOUS_COLUMN:
                    console.error(`Ambiguous column in SQL: ${e.message}`);
                    throw new AmbiguousColumnDbException(`Ambiguous column error : ${e.message}`);
                default:
                    break;
            }

            throw e;
        } finally {
            client.release();
        }
    }

    /**
     * Retrieves a database client from the connection pool.
     * @returns A Promise that resolves to a PoolClient object representing the database client.
     */
    async getDbClient(): Promise<PoolClient> {
        const stats = this.getPoolStats();
        console.log(`[DB Pool] Getting client - Total: ${stats.totalCount}, Idle: ${stats.idleCount}, Waiting: ${stats.waitingCount}`);

        const client = await this.pool.connect();

        const statsAfter = this.getPoolStats();
        console.log(`[DB Pool] Client obtained - Total: ${statsAfter.totalCount}, Idle: ${statsAfter.idleCount}, Waiting: ${statsAfter.waitingCount}`);

        return client;
    }

    /**
     * Releases the database client back to the connection pool.
     * @param client - The database client to release.
     */
    async releaseDbClient(client: PoolClient) {
        const statsBefore = this.getPoolStats();
        console.log(`[DB Pool] Releasing client - Total: ${statsBefore.totalCount}, Idle: ${statsBefore.idleCount}, Waiting: ${statsBefore.waitingCount}`);

        client.release();

        const statsAfter = this.getPoolStats();
        console.log(`[DB Pool] Client released - Total: ${statsAfter.totalCount}, Idle: ${statsAfter.idleCount}, Waiting: ${statsAfter.waitingCount}`);
    }

    /**
     * Gets connection pool statistics for monitoring
     * @returns Pool statistics object
     */
    getPoolStats() {
        return {
            totalCount: this.pool.totalCount,
            idleCount: this.pool.idleCount,
            waitingCount: this.pool.waitingCount
        };
    }

    /**
   * Async Query
   * @param queryStream 
   * @returns 
   */
    async queryStream(client: PoolClient, queryStream: QueryStream): Promise<QueryStream> {
        try {
            const result = client.query(queryStream);
            return result;
        } catch (e: any) {

            switch (e.code) {
                case PostgresError.UNIQUE_VIOLATION:
                    throw new UniqueKeyDbException("Duplicate");
                case PostgresError.FOREIGN_KEY_VIOLATION:
                    throw new ForeignKeyDbException(e.constraint);
                case PostgresError.UNDEFINED_COLUMN:
                    throw new UndefinedColumnDbException(`Undefined column: ${e.message}`);
                case PostgresError.SYNTAX_ERROR:
                    console.error(`Syntax error in SQL: ${e}`);
                    throw new QuerySyntaxErrorDbException(`Query Syntax error`);
                case PostgresError.UNDEFINED_FUNCTION:
                    console.error(`Undefined function in SQL: ${e.message}`);
                    throw new UndefinedFunctionDbException(`Undefined function error : ${e.message}`);
                case PostgresError.AMBIGUOUS_COLUMN:
                    console.error(`Ambiguous column in SQL: ${e.message}`);
                    throw new AmbiguousColumnDbException(`Ambiguous column error : ${e.message}`);
                default:
                    break;
            }
            throw e;
        }
    }
}

const dbClient = new DataSource();
export default dbClient;