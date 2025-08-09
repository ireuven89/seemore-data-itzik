<p align="center">
  <a href="http://nestjs.com/" target="blank"><img src="https://nestjs.com/img/logo-small.svg" width="120" alt="Nest Logo" /></a>
</p>

[circleci-image]: https://img.shields.io/circleci/build/github/nestjs/nest/master?token=abc123def456
[circleci-url]: https://circleci.com/gh/nestjs/nest

  <p align="center">A progressive <a href="http://nodejs.org" target="_blank">Node.js</a> framework for building efficient and scalable server-side applications.</p>
    <p align="center">
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/v/@nestjs/core.svg" alt="NPM Version" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/l/@nestjs/core.svg" alt="Package License" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/dm/@nestjs/common.svg" alt="NPM Downloads" /></a>
<a href="https://circleci.com/gh/nestjs/nest" target="_blank"><img src="https://img.shields.io/circleci/build/github/nestjs/nest/master" alt="CircleCI" /></a>
<a href="https://discord.gg/G7Qnnhy" target="_blank"><img src="https://img.shields.io/badge/discord-online-brightgreen.svg" alt="Discord"/></a>
<a href="https://opencollective.com/nest#backer" target="_blank"><img src="https://opencollective.com/nest/backers/badge.svg" alt="Backers on Open Collective" /></a>
<a href="https://opencollective.com/nest#sponsor" target="_blank"><img src="https://opencollective.com/nest/sponsors/badge.svg" alt="Sponsors on Open Collective" /></a>
  <a href="https://paypal.me/kamilmysliwiec" target="_blank"><img src="https://img.shields.io/badge/Donate-PayPal-ff3f59.svg" alt="Donate us"/></a>
    <a href="https://opencollective.com/nest#sponsor"  target="_blank"><img src="https://img.shields.io/badge/Support%20us-Open%20Collective-41B883.svg" alt="Support us"></a>
  <a href="https://twitter.com/nestframework" target="_blank"><img src="https://img.shields.io/twitter/follow/nestframework.svg?style=social&label=Follow" alt="Follow us on Twitter"></a>
</p>
  <!--[![Backers on Open Collective](https://opencollective.com/nest/backers/badge.svg)](https://opencollective.com/nest#backer)
  [![Sponsors on Open Collective](https://opencollective.com/nest/sponsors/badge.svg)](https://opencollective.com/nest#sponsor)-->

## Description

[Nest](https://github.com/nestjs/nest) framework TypeScript starter repository.

## Project setup

```bash
$ npm install
```

## Compile and run the project

```bash
# development
$ npm run start

# watch mode
$ npm run start:dev

# production mode
$ npm run start:prod
```

## Run tests

```bash
# unit tests
$ npm run test


# test coverage
$ npm run test:cov
```

## Deployment

When you're ready to deploy your NestJS application to production, there are some key steps you can take to ensure it runs as efficiently as possible. Check out the [deployment documentation](https://docs.nestjs.com/deployment) for more information.


# NestJS Snowflake Metadata Sync

A NestJS service for syncing Snowflake metadata (databases, schemas, tables, columns) to MongoDB with incremental updates and comprehensive testing.

## Features

- Connects to Snowflake and discovers all databases, schemas, and tables
- Retrieves detailed column information for each table
- Stores metadata in MongoDB with efficient upsert operations
- Avoids duplicates using checksums to detect changes
- Supports incremental sync (only fetches new/changed tables)
- Comprehensive logging and error handling
- Modular NestJS architecture
- Environment variable configuration
- TypeScript with proper typing
- MongoDB indexes for efficient querying

## Installation

```bash
npm install
```

## Environment Variables

Create a `.env` file in the project root:

```env
# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017/snowflake-metadata

# Snowflake Configuration (only account-level access needed for metadata discovery)
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USERNAME=your-username
SNOWFLAKE_PASSWORD=your-password

# Application
PORT=3000
```

## Usage

1. **Start the application:**
   ```bash
   npm run start:dev
   ```
2. **Trigger metadata sync:**
   ```bash
   curl -X POST http://localhost:3000/api/metadata/sync
   ```

## API

### POST `/api/metadata/sync`
Triggers a sync from Snowflake to MongoDB. Returns a summary of the operation.

#### Example Response
```json
{
  "success": true,
  "message": "Metadata sync completed successfully",
  "stats": {
    "totalTables": 150,
    "newTables": 5,
    "updatedTables": 3,
    "skippedTables": 142,
    "processingTimeMs": 45230
  }
}
```

### GET `/api/metadata/tables/grouped`
Returns all tables grouped by database and schema. This is useful when you only have account details and want to see the complete structure.

#### Example Response
```json
{
  "MY_DATABASE": {
    "PUBLIC": {
      "tables": [
        {
          "database": "MY_DATABASE",
          "schema": "PUBLIC",
          "table": "USERS",
          "columns": [
            {
              "name": "ID",
              "type": "NUMBER",
              "nullable": false,
              "defaultValue": null,
              "comment": "User ID"
            }
          ]
        }
      ],
      "tableCount": 1
    },
    "ANALYTICS": {
      "tables": [],
      "tableCount": 0
    }
  },
  "ANOTHER_DB": {
    "PUBLIC": {
      "tables": [],
      "tableCount": 0
    }
  }
}
```

## Project Structure

```
src/
  app.module.ts
  main.ts
  config/
    database.config.ts
  modules/
    metadata/
      metadata.module.ts
      metadata.controller.ts
      metadata.service.ts
      schemas/
        metadata.schema.ts
        sync.schema.ts
      services/
        snowflake.service.ts
        mongodb.service.ts
      dto/
        sync-response.dto.ts
```

## Testing

- Unit tests for services are in `src/modules/metadata/__tests__`.
- Run all tests:
  ```bash
  npm test
  ```

## Notes

- Ensure your Snowflake user/role has access to all databases and schemas you wish to sync.
- Incremental sync is enabled by default (only new/changed tables are fetched after the first run).
- Sync statistics are automatically saved to MongoDB for tracking and monitoring.
- For advanced usage or troubleshooting, see logs in the application output.
