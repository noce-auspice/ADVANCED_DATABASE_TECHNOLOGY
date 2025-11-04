-- This script creates the necessary tables on both Node_A and Node_B.
-- Run it on each node after connecting to the respective database.

-- Table: Field (Stores information about agricultural fields)


CREATE TABLE Harvest_B (
    harvest_id SERIAL PRIMARY KEY,
    crop_id INTEGER,
    field_id INTEGER,
    harvest_date DATE,
    yield_kg DECIMAL(10,2),
    fragment_flag INTEGER GENERATED ALWAYS AS (crop_id % 2) STORED
);

-- Node_B: 5 rows with odd crop_id  
INSERT INTO Harvest_B (crop_id, field_id, harvest_date, yield_kg) VALUES
(1, 105, '2024-01-18', 1700.30),
(3, 106, '2024-01-25', 2300.40),
(5, 107, '2024-02-05', 1650.90),
(7, 108, '2024-02-12', 1950.60),
(9, 109, '2024-02-18', 2050.70);


-- A2: Database Link & Cross-Node Join (3–10 rows result)

-- Create supporting tables on Node_B
-- Field table on Node_B
CREATE TABLE Field (
    field_id INTEGER PRIMARY KEY,
    field_name VARCHAR(100),
    location VARCHAR(100),
    size_hectares DECIMAL(10,2)
);

INSERT INTO Field VALUES
(101, 'North Field', 'Northern Region', 50.0),
(102, 'South Field', 'Southern Region', 75.5),
(103, 'East Field', 'Eastern Region', 60.2),
(104, 'West Field', 'Western Region', 45.8),
(105, 'Central Field', 'Central Region', 80.0);

-- Crop table on Node_B
CREATE TABLE Crop (
    crop_id INTEGER PRIMARY KEY,
    crop_name VARCHAR(100),
    crop_type VARCHAR(50),
    planting_season VARCHAR(50)
);

INSERT INTO Crop VALUES
(1, 'Maize', 'Cereal', 'Rainy'),
(2, 'Beans', 'Legume', 'Dry'),
(3, 'Wheat', 'Cereal', 'Cool'),
(4, 'Rice', 'Cereal', 'Wet'),
(5, 'Potatoes', 'Tuber', 'Cool');


-- Session 2 (Node_B): Try to update same logical row (will wait)
-- In different session:
BEGIN;
UPDATE dblink('proj_link', 'SELECT * FROM Harvest_B WHERE harvest_id = 1') AS remote_harvest
SET yield_kg = yield_kg + 50;


SELECT dblink_exec(
    'proj_link',
    'UPDATE Harvest_B SET yield_kg = yield_kg + 50 WHERE harvest_id = 1'
);







