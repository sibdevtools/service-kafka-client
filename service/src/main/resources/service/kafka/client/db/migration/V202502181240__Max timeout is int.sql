SET SCHEMA kafka_client_service;

ALTER TABLE bootstrap_group
    ADD COLUMN max_timeout_new INT NOT NULL DEFAULT 30000;

UPDATE bootstrap_group
SET max_timeout_new = CAST(max_timeout AS INT);

ALTER TABLE bootstrap_group
    DROP COLUMN max_timeout;

ALTER TABLE bootstrap_group
    ALTER COLUMN max_timeout_new RENAME TO max_timeout;
