-- Personal-data demo schema for 1CijferHO.
-- Loaded by the postgres container on first start
-- (mounted into /docker-entrypoint-initdb.d via docker-compose).
--
-- secret_sensitive : UUID + encrypted personal identifiers
-- secret_regular   : UUID + demographic fields (1:1 with secret_sensitive)
-- Non-sensitive fields are stored separately as a JSON object in MinIO.

CREATE TABLE IF NOT EXISTS secret_sensitive (
    uuid TEXT PRIMARY KEY,
    persoonsgebonden_nummer TEXT,
    burgerservice_nummer TEXT,
    onderwijs_nummer TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_secret_sensitive_created_at
    ON secret_sensitive(created_at);

CREATE TABLE IF NOT EXISTS secret_regular (
    uuid TEXT PRIMARY KEY,
    geslacht VARCHAR(8),
    nationaliteit_1 VARCHAR(255),
    nationaliteit_2 VARCHAR(255),
    nationaliteit_3 VARCHAR(255),
    geboorteland VARCHAR(255),
    geboorteland_ouder_1 VARCHAR(255),
    geboorteland_ouder_2 VARCHAR(255),
    postcodecijfer_ho VARCHAR(8),
    indicatie_eer_actueel VARCHAR(255),
    indicatie_internationale_student VARCHAR(255),
    nationaliteit_eer_actueel VARCHAR(255),
    herkomstland_cbr VARCHAR(255),
    herkomstindikking_cbr VARCHAR(255),
    indicatie_geboren VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (uuid) REFERENCES secret_sensitive(uuid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_secret_regular_created_at
    ON secret_regular(created_at);
