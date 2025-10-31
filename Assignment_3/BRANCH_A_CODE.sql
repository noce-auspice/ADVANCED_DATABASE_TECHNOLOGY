




-- 1. DDL for Field 
CREATE TABLE Field (
    FieldID SERIAL PRIMARY KEY,
    FieldName VARCHAR(50) NOT NULL,
    SizeHectares DECIMAL(6,2) CHECK (SizeHectares > 0),
    Location VARCHAR(100) NOT NULL,
    SoilType VARCHAR(50) NOT NULL
);

-- 2. DDL Crop 
CREATE TABLE Crop (
    CropID SERIAL PRIMARY KEY,
    FieldID INT  NOT NULL,
    CropName VARCHAR(50) NOT NULL,
    PlantingDate DATE NOT NULL,
    HarvestDate DATE NOT NULL,
    Status VARCHAR(20) CHECK (Status IN ('Planted', 'Growing', 'Harvested', 'Sold')),
    FOREIGN KEY (FieldID) REFERENCES Field(FieldID)
        ON DELETE CASCADE
);

DROP TABLE Crop

-- 3. DDL Fertilizer
CREATE TABLE Fertilizer (
    FertilizerID SERIAL PRIMARY KEY,
    CropID INT NOT NULL,
    Name VARCHAR(50) NOT NULL,
    QuantityUsed DECIMAL(8,2) CHECK (QuantityUsed >= 0),
    Cost DECIMAL(10,2) CHECK (Cost >= 0),
    DateApplied DATE NOT NULL,
    FOREIGN KEY (CropID) REFERENCES Crop(CropID)
);

-- Insert Fields
INSERT INTO Field (FieldName, SizeHectares, Location, SoilType)
VALUES
('North Field', 10.5, 'Kigali', 'Clay'),
('East Field', 8.2, 'Rwamagana', 'Loam'),
('South Field', 12.0, 'Huye', 'Sandy');

-- Insert Crops
INSERT INTO Crop (FieldID, CropName, PlantingDate, HarvestDate, Status)
VALUES
(1, 'Maize', '2025-01-15', '2025-05-20', 'Harvested'),
(2, 'Beans', '2025-02-10', '2025-06-15', 'Harvested'),
(3, 'Rice', '2025-03-01', '2025-07-20', 'Growing'),
(1, 'Cassava', '2025-01-01', '2025-09-30', 'Planted'),
(2, 'Soybean', '2025-03-10', '2025-08-25', 'Planted');

-- Insert Fertilizers
INSERT INTO Fertilizer (CropID, Name, QuantityUsed, Cost, DateApplied)
VALUES
(1, 'NPK', 50, 25000, '2025-02-01'),
(2, 'Urea', 40, 20000, '2025-03-15'),
(3, 'DAP', 60, 30000, '2025-04-10'),
(4, 'Compost', 70, 15000, '2025-03-05'),
(5, 'Organic Mix', 30, 12000, '2025-04-01');


CREATE EXTENSION IF NOT EXISTS postgres_fdw;


-- Create a foreign server (This defines the connection to FleetOperations)

CREATE SERVER BRANCH_B_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'localhost',       -- host where BRANCH_B is running
    dbname 'BRANCH_B',  -- remote db to connect to
    port '5432'
);

-- create a user mapping(Map a local user in BRANCH_B node  to a user in BRANCH_B node)
CREATE USER MAPPING FOR postgres  -- or your local user
SERVER BRANCH_B_server
OPTIONS (
    user 'postgres',         -- BRANCH_B username
    password 'Bobo1999@'       -- BRANCH_B password
);

-- import import  foreign tables from BRANCH_B

IMPORT FOREIGN SCHEMA public
LIMIT TO (Worker, Harvest, Sale)
FROM SERVER BRANCH_B_server INTO public;


SELECT c.CropName, SUM(h.QuantityKG) FROM Crop c
INNER JOIN Harvest h
ON c.CropID = h.CropID
GROUP BY 1


SELECT * FROM Fertilizer

SET max_parallel_workers_per_gather = 0;

-- Enable more parallel workers to improve parallel speed
SET max_parallel_workers_per_gather = 2;
SET max_parallel_workers = 8;
SET max_worker_processes = 8;

Show config_file;

SHOW max_parallel_workers_per_gather;
SHOW max_worker_processes;

SELECT * FROM Harvest

SHOW max_parallel_workers_per_gather;

EXPLAIN ANALYZE
SELECT c.cropname, SUM(h.quantitykg) AS total_harvest
FROM crop c
JOIN harvest h ON c.cropid = h.cropid
GROUP BY c.cropname;

--- Force pararell excecution 
EXPLAIN ANALYZE
SELECT /*+ Parallel */ c.cropname, SUM(h.quantitykg) AS total_harvest
FROM crop c
JOIN harvest h ON c.cropid = h.cropid
GROUP BY c.cropname;


Rollback;

--- TASK 4 

--- Begin the distributed transaction

BEGIN;
---Operation in BRANCH A

INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (1, 'Tomato', '2025-05-01', 'Planted', '2025-10-27');

---Operation in BRANCH B

INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('Peter Mugabo', 'Harvester', '0788000006', 3000);

---Prepare the Transaction for 2PC
PREPARE TRANSACTION 'tx_insert_farm';

---Verify Pending Distributed Transaction
SELECT * FROM pg_prepared_xacts;



BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (1, 'Tomato T1', '2025-05-01', 'Planted', '2025-10-27');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('1', 'Harvester', '0788000101', 3000);
PREPARE TRANSACTION 'tx_farm_1';

BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (2, 'Beans T2', '2025-05-02', 'Planted', '2025-10-28');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('2', 'Planter', '0788000102', 3500);
PREPARE TRANSACTION 'tx_farm_2';

Rollback;


BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (3, 'Rice T3', '2025-05-03', 'Growing', '2025-11-01');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('3', 'Supervisor', '0788000103', 4000);
PREPARE TRANSACTION 'tx_farm_3';

Rollback;

BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (1, 'Cassava T4', '2025-05-04', 'Planted', '2025-11-10');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('4', 'Weeder', '0788000104', 2500);
PREPARE TRANSACTION 'tx_farm_4';

BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (2, 'Soybean T5', '2025-05-05', 'Planted', '2025-11-15');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('5', 'Sprayer', '0788000105', 2800);
PREPARE TRANSACTION 'tx_farm_5';

BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (3, 'Maize T6', '2025-05-06', 'Planted', '2025-11-20');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('6', 'Driver', '0788000106', 3200);
PREPARE TRANSACTION 'tx_farm_6';

Rollback;


BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (1, 'Wheat T7', '2025-05-07', 'Growing', '2025-11-25');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('7', 'Guard', '0788000107', 2700);
PREPARE TRANSACTION 'tx_farm_7';

BEGIN;
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (2, 'Onion T8', '2025-05-08', 'Planted', '2025-12-01');
INSERT INTO worker (fullname, role, contact, dailywage)
VALUES ('8', 'Loader', '0788000108', 2900);
PREPARE TRANSACTION 'tx_farm_8';

INSERT INTO Fertilizer (name, quantity, unit, purchase_date, expiry_date)
VALUES ('Nitrogen Fertilizer', 50, 'kg', '2025-10-01', '2026-10-01');
INSERT INTO Fertilizer (name, quantity, unit, purchase_date, expiry_date)
VALUES ('Phosphorus Fertilizer', 30, 'kg', '2025-10-05', '2026-10-05');
PREPARE TRANSACTION 'tx_farm_9';

select  * from pg_prepared_xacts;
---Prepare the Transaction for 2PC
PREPARE TRANSACTION 'tx_insert_farm';

COMMIT PREPARED 'tx_farm_7';

SELECT * FROM pg_prepared_xacts;

COMMIT PREPARED 'tx_farm_1';
COMMIT PREPARED 'tx_farm_2';
COMMIT PREPARED 'tx_farm_3';
COMMIT PREPARED 'tx_farm_4';
COMMIT PREPARED 'tx_farm_5';
COMMIT PREPARED 'tx_farm_6';
COMMIT PREPARED 'tx_farm_7';
COMMIT PREPARED 'tx_farm_8';
COMMIT PREPARED 'tx_farm_9';
SELECT * FROM pg_prepared_xacts;

ROLLBACK;

SELECT * FROM pg_prepared_xacts;

COMMIT;

SELECT * FROM Field;


--- TASK 6

Rollback;

BEGIN;
UPDATE crop SET status = 'Harvesting' WHERE fieldid = 1 AND cropname = 'Tomato T1';
-- Do not commit yet, hold the lock

BEGIN;
UPDATE crop SET status = 'Growing' WHERE fieldid = 1 AND cropname = 'Tomato T1';
-- This will hang until Session 1 commits or rolls back

SELECT pid, locktype, relation::regclass, mode, granted
FROM pg_locks
WHERE relation = (SELECT oid FROM pg_class WHERE relname = 'crop')
AND pid IN (SELECT pid FROM pg_stat_activity WHERE usename = CURRENT_USER);

--- TASK 7

-- Enable parallel execution
SET max_parallel_workers_per_gather = 4;
SET max_parallel_workers = 8;

-- Parallel insert (simulated large dataset)
INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
SELECT generate_series(1, 10000), 'Tomato_' || generate_series(1, 10000), 
       '2025-05-01', 'Planted', '2025-10-27'
ON CONFLICT DO NOTHING;

Rollback;

-- Parallel aggregation
EXPLAIN ANALYZE
SELECT cropname, COUNT(*) 
FROM crop 
WHERE plantingdate >= '2025-05-01' 
GROUP BY cropname;



SELECT fieldid FROM field;
INSERT INTO field (fieldid, name)
VALUES (4, 'Field 4');

INSERT INTO crop (fieldid, cropname, plantingdate, status, harvestdate)
VALUES (4, 'Cassava T4', '2025-05-04', 'Planted', '2025-11-10');

SELECT * FROM Crop

--- TASK 8
-- Enable dblink extension
CREATE EXTENSION IF NOT EXISTS dblink;

-- Connect to a remote database (adjust connection string)
SELECT dblink_connect('myconn', 'dbname=BRANCH_B host=localhost port=5432 user=postgres password=Bobo1999@');

-- Query remote data
SELECT * FROM dblink('myconn', 'SELECT * FROM crop')
AS t(fieldid INT, cropname TEXT, plantingdate DATE, status TEXT, harvestdate DATE)
LIMIT 10;

SELECT dblink_get_connections();



--- TASK 9
-- Distributed join
EXPLAIN PLAN FOR
SELECT a.cropname, b.cropname
FROM crop_A a
JOIN crop_B@branchB_link b
ON a.status = b.status;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- The optimizer attempts to minimize data movement by executing 
-- filters locally before transferring data across the link.

-- Centralized query
SELECT COUNT(*) FROM farm_transaction;

-- Parallel query
SELECT /*+ PARALLEL(farm_transaction, 8) */ COUNT(*) FROM farm_transaction;

-- Distributed query (using dblink)
SELECT COUNT(*) FROM farm_transaction@branchB_link;

-- Collect timing and I/O statistics
SET AUTOTRACE ON;














