-- Finomics AWS: service catalog (parity with Azure ``service_configuration_v2``, table name ``service_configuration``).
-- Run once per database. Adjust Iceberg table names to match your Glue/Iceberg deployment.

CREATE TABLE IF NOT EXISTS service_configuration (
    id SERIAL PRIMARY KEY,
    service_code VARCHAR(50) NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    cloud_provider VARCHAR(20) NOT NULL,
    category VARCHAR(100),
    iceberg_metrics_table VARCHAR(255),
    iceberg_resources_table VARCHAR(255),
    recommender_class VARCHAR(255),
    sku_json_file VARCHAR(255),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    display_service_name VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_service_configuration_code_cloud UNIQUE (service_code, cloud_provider)
);

CREATE INDEX IF NOT EXISTS idx_service_configuration_cloud_active
    ON service_configuration (LOWER(TRIM(cloud_provider)), is_active);

COMMENT ON TABLE service_configuration IS 'AWS/Azure service metadata; EC2 bronze resolves ACTIVE_SERVICES=ALL from this table for cloud_provider=aws';
