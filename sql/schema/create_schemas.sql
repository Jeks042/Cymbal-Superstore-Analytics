-- SQL Schema Design for a Data Warehouse

-- raw database schema for storing processed data from data ingestion pipelines
CREATE SCHEMA IF NOT EXISTS raw;

-- analytics database schema for storing analytics data/query results
CREATE SCHEMA IF NOT EXISTS analytics;