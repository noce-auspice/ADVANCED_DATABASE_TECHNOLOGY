-- This script creates the necessary tables on both Node_A and Node_B.
-- Run it on each node after connecting to the respective database.

-- Table: Field (Stores information about agricultural fields)
CREATE TABLE Field (
    field_id INTEGER PRIMARY KEY,
    field_name VARCHAR(100) NOT NULL,
    location VARCHAR(150),
    size_acres NUMERIC
);

-- Table: Crop (Stores types of crops)
CREATE TABLE Crop (
    crop_id INTEGER PRIMARY KEY,
    crop_name VARCHAR(100) NOT NULL,
    season VARCHAR(50)
);

-- Table: Harvest (Main fact table for harvest records)
-- We will fragment this table later in A1.
CREATE TABLE Harvest (
    harvest_id INTEGER PRIMARY KEY,
    field_id INTEGER NOT NULL,
    crop_id INTEGER NOT NULL,
    harvest_date DATE NOT NULL,
    yield_kg NUMERIC NOT NULL CHECK (yield_kg > 0),
    CONSTRAINT fk_harvest_field FOREIGN KEY (field_id) REFERENCES Field(field_id),
    CONSTRAINT fk_harvest_crop FOREIGN KEY (crop_id) REFERENCES Crop(crop_id)
);


-- Create Harvest_B table on Node_B
-- This table will hold fragment B of the Harvest data

CREATE TABLE Harvest_B (
    harvest_id INTEGER PRIMARY KEY,
    field_id INTEGER NOT NULL,
    crop_id INTEGER NOT NULL,
    harvest_date DATE NOT NULL,
    yield_kg NUMERIC NOT NULL CHECK (yield_kg > 0),
    CONSTRAINT fk_harvest_field_b FOREIGN KEY (field_id) REFERENCES Field(field_id),
    CONSTRAINT fk_harvest_crop_b FOREIGN KEY (crop_id) REFERENCES Crop(crop_id)
);

-- Insert some sample data into Field and Crop (Total committed rows for project: 4)
-- These are not part of the 10-row budget for Harvest, but are necessary for joins.
INSERT INTO Field VALUES (1, 'North Field', 'Valley Region', 50);
INSERT INTO Field VALUES (2, 'South Field', 'Hill Region', 30);
INSERT INTO Crop VALUES (101, 'Maize', 'Rainy');
INSERT INTO Crop VALUES (102, 'Beans', 'Dry');

-- Insert 5 rows into Harvest_B (on Node_B via dblink)
INSERT INTO Harvest_B VALUES (6, 1, 102, '2023-09-05', 200);
INSERT INTO Harvest_B VALUES (7, 2, 101, '2023-09-10', 400);
INSERT INTO Harvest_B VALUES (8, 2, 101, '2023-09-15', 410);
INSERT INTO Harvest_B VALUES (9, 1, 102, '2023-10-01', 210);
INSERT INTO Harvest_B VALUES (10, 2, 102, '2023-10-05', 220);

-- Verify the data was inserted
SELECT * FROM Harvest_B;



