export class DatabaseConfig {
  static getMongoUri(): string {
    return process.env.MONGODB_URI || 'mongodb://localhost:27017/snowflake-metadata';
  }
  
  static getSnowflakeConfig() {
    return {
      account: process.env.SNOWFLAKE_ACCOUNT ?? '',
      username: process.env.SNOWFLAKE_USERNAME ?? '',
      password: process.env.SNOWFLAKE_PASSWORD ?? '',
    };
  }
}