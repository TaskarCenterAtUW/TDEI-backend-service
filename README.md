# TDEI-data-query-service
The Backend Service is responsible for processing dataset query operations, including spatial and non-spatial tasks such as bounding box operations, spatial joins, road information tagging, and dataset unions. This service ensures that the required dataset manipulations are performed efficiently, and it returns OSW-compliant datasets as output, enabling seamless integration with TDEI system components.

## Getting Started
The project is built on NodeJS framework. All the regular nuances for a NodeJS project are valid for this.

## System requirements
| Software | Version| 
|----|---|
| NodeJS | 16.17.0|
| Typescript | 4.8.2|


## Environment variables

Application configuration is read from .env file. Below are the list of environemnt variables service is dependent on. An example of environment file is available [here](./env.example) and description of environment variable is presented in below table

|Name| Description |
|--|--|
| PROVIDER | Provider for cloud service or local (optional)|
|QUEUECONNECTION | Queue connection string |
|STORAGECONNECTION | Storage connection string|
|PORT |Port on which application will run|
|AUTH_HOST | TDEI Auth service url|
|POSTGRES_DB | Database name|
|POSTGRES_HOST| Link to the database host |
|POSTGRES_USER| Database user |
|POSTGRES_PASSWORD| Database user password|
|SSL| Database ssl flag|
|OSW_SCHEMA_URL | OSW Schema Url|
|BACKEND_RESPONSE_TOPIC| Response topic on which result to be announced |

## Build

Follow the steps to install the node packages required for both building and running the application

1. Install the dependencies. Run the following command in terminal on the same directory level as `package.json`
    ```shell
    npm install
    ```
2. To start the server, use the command `npm run start`
3. The http server by default starts with 3000 port or whatever is declared in `process.env.PORT` (look at `index.ts` for more details)
4. Health check available at path `health/ping` with get and post. Make `get` or `post` request to `http://localhost:3000/health/ping`.
Ping should respond with "healthy!" message with HTTP 200 status code.

## Test

Follow the steps to install the node packages required for testing the application

1. Ensure we have installed the dependencies. Run the following command in terminal on the same directory level as `package.json`
    ```shell
    npm install
    ```
2. To start testing suits, use the command `npm test` , this command will execute all the unit test suites defined for application.


## Service request signatures documentation

Service request signatures are defined under [Service signatures](./src/services.json)

## Subscription 
Subscriptions to listening topics are configured in the `/src/subscription.json` file. This file defines the topics that the system listens to for incoming messages.

## Message bbox Request 

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "bbox_intersect",
      "parameters": {
        "tdei_dataset_id" : "tdei_dataset_id",
        "bbox" : [1,2,3,4]
      },
      "user_id": "user_id",
    } 
}
```

## Message bbox Response 

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "file path",
      "parameters":{...},
      "file_upload_path" : "zip file path",
      "success": true|false,
      "message": "message" // if false the error string else empty string
    } 
  }
```

