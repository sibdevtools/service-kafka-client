SET SCHEMA kafka_client_service;

CREATE TABLE message_template_header
(
    message_template_id BIGINT        NOT NULL,
    header_name         VARCHAR(512)  NOT NULL,
    header_value        VARCHAR(1024) NOT NULL,
    FOREIGN KEY (message_template_id) REFERENCES message_template (id)
);