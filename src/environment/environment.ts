import dotenv from 'dotenv';
dotenv.config();
/**
 * Contains all the configurations required for setting up the core project
 * While most of the parameters are optional, appInsights connection is 
 * a required parameter since it is auto imported in the `tdei_logger.ts`
 */
export const environment = {
    appName: process.env.npm_package_name,
    eventBus: {
        backendResponseTopic: process.env.BACKEND_RESPONSE_TOPIC,
        maxConcurrentMessages: parseInt(process.env.MAX_CONCURRENT_MESSAGES ?? "2"),
    },
    database: {
        username: process.env.POSTGRES_USER,
        host: process.env.POSTGRES_HOST,
        password: process.env.POSTGRES_PASSWORD,
        database: process.env.POSTGRES_DB,
        ssl: Boolean(process.env.SSL),
        port: parseInt(process.env.POSTGRES_PORT ?? "5432"),
    },
    appPort: parseInt(process.env.PORT ?? "8080"),
    authPermissionUrl: process.env.AUTH_HOST + '/api/v1/hasPermission',
    secretGenerateUrl: process.env.AUTH_HOST + '/api/v1/generateSecret',
    oswSchemaUrl: process.env.OSW_SCHEMA_URL,
}