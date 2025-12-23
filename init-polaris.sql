-- Ensure the schema exists before Polaris tries to use it
CREATE SCHEMA IF NOT EXISTS polaris_schema;
-- Grant the polaris user permissions to manage it
ALTER SCHEMA polaris_schema OWNER TO polaris;