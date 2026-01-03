-- Sample PostgreSQL Schema for Animal Database
-- No foreign keys or constraints (except primary keys)

-- Animals table
CREATE TABLE animals (
    animal_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    species VARCHAR(100),
    age INTEGER,
    weight_kg NUMERIC(8,2),
    habitat_name VARCHAR(100),
    last_checkup DATE
);

-- Habitats table
CREATE TABLE habitats (
    habitat_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    climate VARCHAR(50),
    size_acres NUMERIC(10,2),
    capacity INTEGER,
    built_date DATE
);

-- Feedings table
CREATE TABLE feedings (
    feeding_id INTEGER PRIMARY KEY,
    animal_name VARCHAR(100),
    food_type VARCHAR(100),
    quantity_kg NUMERIC(6,2),
    feeding_time TIMESTAMP,
    fed_by VARCHAR(100)
);
