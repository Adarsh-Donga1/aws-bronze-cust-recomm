-- EC2 (AWS) — idempotent insert into service_configuration
-- Table: service_configuration  (not service_configuration_v2; aligns with Azure column semantics)
--
-- 1) Create table (once):  psql -f 00_create_service_configuration.sql
-- 2) Load this row:         psql -f cr_ec2.sql
--
-- Update iceberg_* columns if your deployment uses different table names (see bronze/services/aws/ec2.py).

-- Remove existing EC2 + aws row
DELETE FROM service_configuration
 WHERE service_code = 'EC2'
   AND LOWER(TRIM(cloud_provider)) = 'aws';

-- Insert EC2 service (inventory + daily metrics in Iceberg)
INSERT INTO service_configuration (
    service_code,
    service_name,
    cloud_provider,
    category,
    iceberg_metrics_table,
    iceberg_resources_table,
    recommender_class,
    sku_json_file,
    is_active,
    display_service_name
) VALUES (
    'EC2',
    'EC2',
    'aws',
    'Compute',
    'bronze_aws_metrics_v1',
    'bronze_aws_ec2_instances_v1',
    NULL,
    NULL,
    TRUE,
    'Amazon EC2'
);
