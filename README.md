# TDEI-data-query-service
The Backend Service is responsible for processing dataset query operations, including spatial and non-spatial tasks such as bounding box operations, spatial joins, road information tagging, dataset unions, self-merge and OSW-to-OSM queries. This service ensures that the required dataset manipulations are performed efficiently, and it returns OSW-compliant datasets as output, enabling seamless integration with TDEI system components.

## Getting Started
The project is built on NodeJS framework. All the regular nuances for a NodeJS project are valid for this.

## System requirements
| Software | Version|
|----|---|
| NodeJS | 24 (the Docker image uses `node:24.19.0-alpine`)|
| Typescript | 5.x|


## Environment variables

Application configuration is read from .env file. Below are the list of environemnt variables service is dependent on. An example of environment file is available [here](./.env.example) and description of environment variable is presented in below table

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
|POSTGRES_PORT| Database port (default 5432)|
|SSL| Database ssl flag|
|MAX_CONCURRENT_MESSAGES| How many backend requests are processed at once (default 2)|
|OSW_SCHEMA_URL | OSW Schema Url|
|BACKEND_RESPONSE_TOPIC| Response topic on which result to be announced |

## Build

Follow the steps to install the node packages required for both building and running the application

1. Install the dependencies. Run the following command in terminal on the same directory level as `package.json`
    ```shell
    npm install
    ```
2. To start the server, use the command `npm run start`
3. The http server by default starts with 8080 port or whatever is declared in `process.env.PORT` (see `src/environment/environment.ts`)
4. Health check available at path `health/ping` with get and post. Make `get` or `post` request to `http://localhost:8080/health/ping`.
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

### Dataset union (`union_dataset`, union 5.0)

Combines two OSW datasets into one. Nothing is implicit: every snap, dedup and junction
comes from the request's `config`, and a request with no config returns the two datasets
combined, unchanged.

| Parameter | Required | Meaning |
|---|---|---|
| `tdei_dataset_id_one` | yes | First dataset, the default anchor (its version of a shared feature is kept) |
| `tdei_dataset_id_two` | yes | Second dataset; must differ from the first |
| `config` | no | A JSON object, or a JSON string of one, with up to four sections |

| Section | Modes | What it does | Settings |
|---|---|---|---|
| `snap` | off / default / custom | joins a node to a nearby node of the same kind in the other dataset | `proximity`, `anchor`, `attributes`, `coincident`, `others`, `rules` |
| `dedup` | off / default / custom | removes a line that duplicates an anchor line | `proximity`, `min_overlap`, `max_angle`, `anchor`, `rules` |
| `connect` | off / default / custom | makes junctions between two kinds of way, in rule order | `proximity`, `rules` (each with `operation` `point` or `geometry`, `node_tags`, `edge_tags`) |
| `extensions` | off / default | merges points, lines, buildings and zones | one block per type: `points` {`enabled`, `proximity`}, `lines` {`enabled`, `proximity`, `min_overlap`}, `polygons` {`enabled`, `min_overlap`}, `zones` {`enabled`, `min_overlap`} |

A section left out is off. The 4.x parameters `proximity` and `entity_filters` are rejected.

```json
{
  "tdei_dataset_id_one": "<id>",
  "tdei_dataset_id_two": "<id>",
  "config": {
    "snap":       { "mode": "default", "proximity": 1 },
    "dedup":      { "mode": "default", "proximity": 1 },
    "connect":    { "mode": "custom", "rules": [
                    { "name": "kerbed-crossing", "from": "crossing", "to": "sidewalk", "operation": "geometry",
                      "proximity": 1, "node_tags": { "barrier": "kerb", "kerb": "lowered" } } ] },
    "extensions": { "mode": "default", "points": { "proximity": 1 }, "lines": { "proximity": 1 } }
  }
}
```

**How a request is checked.** The service checks the envelope before opening a transaction:
- the config is a JSON object of known sections, at most 64 KB;
- `proximity` and `entity_filters` are not sent;
- the two dataset ids differ.

The database function then validates every rule (`content.union5_validate_config`, run first
inside `content.tdei_union_dataset`). It rejects a bad config with a message naming the
stage, rule and field, for example `config.connect.rules[1].operation: is required — one of
point, geometry`. That message is returned as the job's error.

**Output.** A zip with the six `osw.*.geojson` files and `union_report.json`, the union's run
report: the effective config, what each rule did, totals, warnings and seconds per stage.

**Database.** The union 5.0 functions (`test/union-5.0-harness/union/union_5_0.sql`) must be
deployed; deploying them drops the 4.x signatures. One job is one transaction, so
`statement_timeout` and `idle_in_transaction_session_timeout` must be off or longer than the
longest job. At about 0.25 ms per input edge, 3.66 million edges take about 16 minutes and
7 GB of temporary space.

| Read | For |
|---|---|
| [docs/union-FAQs.md](./docs/union-FAQs.md) | every setting, the default rule sets, limits, error messages, migrating a 4.x request |
| [docs/union-report.md](./docs/union-report.md) | the run report, key by key, and how to read one |
| [union-configurator/dist/union-configurator.html](./union-configurator/dist/union-configurator.html) | building a config without writing JSON (open it in a browser) |
| [test/union-5.0-harness](./test/union-5.0-harness/README.md) | the test harness, the demo (run any config on a sample neighbourhood) and performance at scale |

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

## Message union Request

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "union_dataset",
      "parameters": {
        "tdei_dataset_id_one" : "tdei_dataset_id",
        "tdei_dataset_id_two" : "tdei_dataset_id",
        "config" : { "snap": { "mode": "default", "proximity": 1 }, "dedup": { "mode": "default", "proximity": 1 } }
      },
      "user_id": "user_id"
    }
}
```

The response has the same shape as the bbox response above, with `file_upload_path` pointing
to the zip. When the request is rejected, `success` is false and `message` is the reason (for
example the union's own message for a bad config).
