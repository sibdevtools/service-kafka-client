CREATE SCHEMA IF NOT EXISTS kafka_client_service;

SET SCHEMA kafka_client_service;

CREATE TABLE bootstrap_group
(
    id          BIGINT                  NOT NULL AUTO_INCREMENT,
    code        VARCHAR_IGNORECASE(255) NOT NULL UNIQUE,
    name        VARCHAR(512)            NOT NULL,
    max_timeout BIGINT                  NOT NULL,
    created_at  TIMESTAMP               NOT NULL,
    modified_at TIMESTAMP               NOT NULL,
    CONSTRAINT bootstrap_group_pk PRIMARY KEY (id)
);

CREATE TABLE bootstrap_group_servers
(
    bootstrap_group_id BIGINT       NOT NULL,
    server             VARCHAR(512) NOT NULL,
    FOREIGN KEY (bootstrap_group_id) REFERENCES bootstrap_group (id)
);