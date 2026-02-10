CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

INSERT INTO test_table (name) VALUES ('Alice'), ('Bob'), ('Charlie');
