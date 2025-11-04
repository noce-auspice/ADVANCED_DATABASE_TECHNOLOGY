-- Agricultural Distributed Database Management System 

-- Project Overview

-- This project implements a distributed agricultural database management system using PostgreSQL 
-- with horizontal fragmentation, distributed queries, and business rule enforcement. The system manages 
-- farm data across multiple database nodes with comprehensive data integrity and performance 
-- optimization.


-- Case Study: Agricultural Harvest Management
-- Business Context
-- Farm Operations: Tracks agricultural fields, crop types, and harvest
-- yields across distributed database nodes

-- Distributed Architecture: Data horizontally fragmented across Node_A 
-- and Node_B for performance and scalability

-- Data Integrity: Ensures ACID properties across distributed fragments using two-phase commit


RELATIONSHIP SUMMARY


-- Field (1) ────── (M) Harvest (M) ────── (1) Crop
--                     │
--                     ├─ (1:1) Harvest_A (Node_A Fragment)
--                     ├─ (1:1) Harvest_B (Node_B Fragment)
--                     ├─ (M:1) Crop_AUDIT (Audit Trail)
--                     └─ (M:1) BUSINESS_LIMITS (Rule Enforcement)

-- HIER Table (Hierarchical):
-- Farm (1000) ──has_field──→ Field (1,2) ──grows_crop──→ Crop (101,102) ──produces──→ Harvest (1-6)

-- TRIPLE Table (Semantic):
-- Entity ──isA──→ Type ──isA──→ SuperType (Transitive Inheritance)
-- Entity ──hasSeason──→ Season (Attribute Assignment)
-- Harvest ──ofType──→ Crop (Instance Classification)



--  SYSTEM ARCHITECTURE & IMPLEMENTATION
 
-- A1: Horizontal Fragmentation & Recombination
-- Objective: Split the main Harvest table into horizontal fragments based on 
-- harvest_id range and create a unified view that transparently combines both fragments.

-- Technical Approach:

-- Range-based partitioning on harvest_id (1-5 on Node_A, 6-10 on Node_B)

-- Uses PostgreSQL dblink extension for cross-node queries

-- Creates a unified view that appears as a single table to applications

-- Implements data validation through checksums and row counts

-- This script creates the necessary tables on both Node_A and Node_B.
-- Run it on each node after connecting to the respective database.

-- Enable dblink extension
CREATE EXTENSION IF NOT EXISTS dblink;

-- Create connection to Node_B (simulated)
-- In real scenario, replace with actual connection string

SELECT dblink_connect(
    'proj_link',
    'dbname=NOD_B user=postgres password=Bobo1999@'
);

-- A1: Fragment & Recombine Main Fact (≤10 rows)

-- Create horizontally fragmented tables
CREATE TABLE Harvest_A (
    harvest_id SERIAL PRIMARY KEY,
    crop_id INTEGER,
    field_id INTEGER,
    harvest_date DATE,
    yield_kg DECIMAL(10,2),
    -- Deterministic rule: Even crop_id goes to A, Odd to B
    fragment_flag INTEGER GENERATED ALWAYS AS (crop_id % 2) STORED
);

-- Insert ≤10 total rows split across fragments
-- Node_A: 5 rows with even crop_id
INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) VALUES
(2, 101, '2024-01-15', 1500.50),
(4, 102, '2024-01-20', 2200.75),
(6, 103, '2024-02-01', 1800.25),
(8, 101, '2024-02-10', 1900.00),
(10, 104, '2024-02-15', 2100.80);


-- Create view combining both fragments (simulated remote access)
CREATE VIEW Harvest_ALL AS
SELECT * FROM Harvest_A
UNION ALL
SELECT * FROM dblink('proj_link', 'SELECT * FROM Harvest_B') AS remote_harvest(
    harvest_id INTEGER,
    crop_id INTEGER,
    field_id INTEGER,
    harvest_date DATE,
    yield_kg DECIMAL(10,2),
    fragment_flag INTEGER
);


-- Validate counts and checksum
SELECT 'Harvest_A' as fragment, COUNT(*) as row_count, SUM(MOD(harvest_id, 97)) as checksum FROM Harvest_A
UNION ALL
SELECT 'Harvest_B' as fragment, COUNT(*) as row_count, SUM(MOD(harvest_id, 97)) as checksum FROM dblink('proj_link', 'SELECT * FROM Harvest_B') AS remote_harvest(
    harvest_id INTEGER,
    crop_id INTEGER,
    field_id INTEGER,
    harvest_date DATE,
    yield_kg DECIMAL(10,2),
    fragment_flag INTEGER
)
UNION ALL
SELECT 'Harvest_ALL' as fragment, COUNT(*) as row_count, SUM(MOD(harvest_id, 97)) as checksum FROM Harvest_ALL;


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

-- A2.1: Remote SELECT on Field table (up to 5 rows)

SELECT * FROM dblink('proj_link', 'SELECT * FROM Field LIMIT 5') AS remote_field(
    field_id INTEGER,
    field_name VARCHAR(100),
    location VARCHAR(100),
    size_hectares DECIMAL(10,2)
);

-- A2.2: Distributed join: Harvest_A joined with remote Crop

SELECT 
    h.harvest_id, 
    h.crop_id, 
    c.crop_name, 
    h.harvest_date, 
    h.yield_kg
FROM Harvest_A h
JOIN (
    SELECT * 
    FROM dblink('proj_link', 'SELECT crop_id, crop_name FROM Crop')
    AS remote_crop(
        crop_id INTEGER,
        crop_name VARCHAR(100)
    )
) AS c
ON h.crop_id = c.crop_id
WHERE h.crop_id IN (2, 4, 6)
LIMIT 10;


-- A3: Parallel vs Serial Aggregation (≤10 rows data)

-- SERIAL aggregation
SELECT crop_id, COUNT(*) as harvest_count, SUM(yield_kg) as total_yield
FROM Harvest_ALL
GROUP BY crop_id
HAVING COUNT(*) BETWEEN 1 AND 3  -- Ensure 3-10 groups
ORDER BY crop_id;

-- PARALLEL aggregation (PostgreSQL automatically parallelizes)
-- We can use hints or force parallel mode
SET max_parallel_workers_per_gather = 4;

SELECT /*+ Parallel(harvest_all, 4) */ 
       crop_id, COUNT(*) as harvest_count, SUM(yield_kg) as total_yield
FROM Harvest_ALL
GROUP BY crop_id
HAVING COUNT(*) BETWEEN 1 AND 3
ORDER BY crop_id;

-- Reset parallel workers
RESET max_parallel_workers_per_gather;

-- Get execution plans
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT crop_id, COUNT(*) as harvest_count, SUM(yield_kg) as total_yield
FROM Harvest_ALL
GROUP BY crop_id
HAVING COUNT(*) BETWEEN 1 AND 3;


-- A4: Two-Phase Commit & Recovery (2 rows)

-- Create supporting tables
CREATE TABLE Crop_Inventory (
    inventory_id SERIAL PRIMARY KEY,
    crop_id INTEGER,
    quantity_kg DECIMAL(10,2),
    storage_date DATE
);

-- PL/SQL block equivalent in PostgreSQL (using transaction)
DO $$
BEGIN
    -- Insert local row
    INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
    VALUES (12, 110, '2024-03-01', 2400.00);
    
    -- Insert remote row (simulated)
    PERFORM dblink_exec('proj_link', 
        'INSERT INTO Crop_Inventory (crop_id, quantity_kg, storage_date) 
         VALUES (12, 2400.00, ''2024-03-01'')');
    
    -- Commit both (simulating 2PC)
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Transaction failed: %', SQLERRM;
END $$;

-- Verify consistency
SELECT 'Local' as location, COUNT(*) as row_count FROM Harvest_A WHERE crop_id = 12
UNION ALL
SELECT 'Remote' as location, COUNT(*) as row_count FROM dblink('proj_link', 
    'SELECT COUNT(*) FROM Crop_Inventory WHERE crop_id = 12') AS remote_count(count_val INTEGER);


-- A5: Distributed Lock Conflict & Diagnosis

-- Session 1 (Node_A): Open transaction and lock a row
BEGIN;
UPDATE Harvest_A SET yield_kg = yield_kg + 100 WHERE harvest_id = 1;

-- Session 2 (Node_B): Try to update same logical row (will wait)
-- In different session:
SELECT dblink_exec(
    'proj_link',
    'UPDATE Harvest_B SET yield_kg = yield_kg + 50 WHERE harvest_id = 1'
);


-- Lock diagnostics (PostgreSQL system views)
SELECT 
    locktype, 
    relation::regclass, 
    mode, 
    granted,
    pg_blocking_pids(pid) as blocking_pids
FROM pg_locks 
WHERE relation = 'harvest_a'::regclass;

-- Release lock from Session 1
COMMIT;

-- B6: Declarative Rules Hardening (≤10 committed rows)

-- Add constraints to Crop and Harvest tables
ALTER TABLE Crop 
ADD CONSTRAINT chk_crop_id_positive CHECK (crop_id > 0),
ADD CONSTRAINT chk_crop_name_not_null CHECK (crop_name IS NOT NULL),
ADD CONSTRAINT chk_valid_season CHECK (planting_season IN ('Rainy', 'Dry', 'Cool', 'Wet'));

ALTER TABLE Harvest_A
ADD CONSTRAINT chk_yield_positive CHECK (yield_kg > 0),
ADD CONSTRAINT chk_harvest_date_not_null CHECK (harvest_date IS NOT NULL),
ADD CONSTRAINT chk_future_harvest CHECK (harvest_date <= CURRENT_DATE);

-- Test INSERTs with proper error handling
DO $$
BEGIN
    -- Passing INSERTs
    INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
    VALUES (14, 111, '2024-03-10', 2600.00);
    
    PERFORM dblink_exec('proj_link',
        'INSERT INTO Crop (crop_id, crop_name, crop_type, planting_season)
         VALUES (14, ''Sorghum'', ''Cereal'', ''Dry'')');

    -- Failing INSERTs (wrapped to prevent commit)
    BEGIN
        INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
        VALUES (15, 112, '2024-03-15', -100.00); -- Negative yield
        RAISE NOTICE 'This should not print - constraint should fail';
    EXCEPTION 
        WHEN check_violation THEN
            RAISE NOTICE 'Caught expected constraint violation: Negative yield';
    END;
    
    BEGIN
        PERFORM dblink_exec('proj_link',
            'INSERT INTO Crop (crop_id, crop_name, crop_type, planting_season)
             VALUES (15, NULL, ''Vegetable'', ''Unknown'')'); -- NULL name, invalid season
        RAISE NOTICE 'This should not print - constraint should fail';
    EXCEPTION 
        WHEN OTHERS THEN
            RAISE NOTICE 'Caught expected constraint violation: NULL name or invalid season';
    END;

    COMMIT;
END $$;


-- Verify only passing rows committed
SELECT 'Committed rows check - total should be ≤10' as verification;
SELECT COUNT(*) as total_harvest_rows FROM (
    SELECT * FROM Harvest_A 
    UNION ALL 
    SELECT * FROM dblink('proj_link', 'SELECT * FROM Harvest_B') AS remote_harvest(
        harvest_id INTEGER, crop_id INTEGER, field_id INTEGER, 
        harvest_date DATE, yield_kg DECIMAL(10,2), fragment_flag INTEGER
    )
) all_harvest;


-- B7: E-C-A Trigger for Denormalized Totals

-- Create audit table
CREATE TABLE Crop_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bef_total DECIMAL(15,2),
    aft_total DECIMAL(15,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64)
);

-- Create trigger function
CREATE OR REPLACE FUNCTION update_crop_totals()
RETURNS TRIGGER AS $$
DECLARE
    before_total DECIMAL(15,2);
    after_total DECIMAL(15,2);
BEGIN
    -- Get before total
    SELECT COALESCE(SUM(yield_kg), 0) INTO before_total 
    FROM Harvest_A 
    WHERE crop_id = COALESCE(OLD.crop_id, NEW.crop_id);
    
    -- Get after total (for INSERT/UPDATE)
    IF TG_OP != 'DELETE' THEN
        SELECT COALESCE(SUM(yield_kg), 0) INTO after_total 
        FROM Harvest_A 
        WHERE crop_id = NEW.crop_id;
    ELSE
        after_total := 0;
    END IF;
    
    -- Log to audit table
    INSERT INTO Crop_AUDIT (bef_total, aft_total, key_col)
    VALUES (before_total, after_total, 'Crop_' || COALESCE(OLD.crop_id, NEW.crop_id));
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER harvest_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON Harvest_A
    FOR EACH STATEMENT
    EXECUTE FUNCTION update_crop_totals();

-- Test with mixed DML (affecting ≤4 rows total)
BEGIN;
    INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) VALUES (16, 113, '2024-03-20', 2700.00);
    UPDATE Harvest_A SET yield_kg = yield_kg * 1.1 WHERE harvest_id = 2;
    DELETE FROM Harvest_A WHERE harvest_id = 3;
    UPDATE Harvest_A SET field_id = 114 WHERE harvest_id = 4;
COMMIT;

-- Show audit results
SELECT * FROM Crop_AUDIT ORDER BY changed_at;

-- Show current totals
SELECT crop_id, SUM(yield_kg) as current_total 
FROM Harvest_A 
GROUP BY crop_id 
ORDER BY crop_id;


-- B8: Recursive Hierarchy Roll-Up (6–10 rows)

-- Create hierarchy table for crop classification

CREATE TABLE Crop_Hierarchy (
    parent_id INTEGER,
    child_id INTEGER,
    relationship_type VARCHAR(50)
);

-- Insert 6-10 rows forming 3-level hierarchy
INSERT INTO Crop_Hierarchy VALUES
(NULL, 1, 'root'),           -- Level 1: Root (Cereals)
(1, 2, 'subcategory'),       -- Level 2: Maize
(1, 3, 'subcategory'),       -- Level 2: Wheat
(1, 4, 'subcategory'),       -- Level 2: Rice
(2, 5, 'variety'),           -- Level 3: Sweet Corn
(2, 6, 'variety'),           -- Level 3: Field Corn
(3, 7, 'variety'),           -- Level 3: Winter Wheat
(3, 8, 'variety'),           -- Level 3: Spring Wheat
(4, 9, 'variety'),           -- Level 3: Basmati Rice
(4, 10, 'variety');          -- Level 3: Jasmine Rice

-- Recursive query to traverse hierarchy

WITH RECURSIVE CropTree AS (
    -- Anchor: Root nodes
    SELECT 
        child_id, 
        child_id AS root_id, 
        0 AS depth,
        CAST(child_id AS TEXT) AS path
    FROM Crop_Hierarchy 
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: Child nodes
    SELECT 
        ch.child_id,
        ct.root_id,
        ct.depth + 1,
        ct.path || '->' || ch.child_id
    FROM Crop_Hierarchy ch
    JOIN CropTree ct ON ch.parent_id = ct.child_id
)
SELECT 
    ct.child_id,
    ct.root_id,
    ct.depth,
    c.crop_name,
    ct.path
FROM CropTree ct
LEFT JOIN dblink('proj_link', 'SELECT crop_id, crop_name FROM Crop')
    AS c(crop_id INTEGER, crop_name VARCHAR(100))
    ON ct.child_id = c.crop_id
ORDER BY ct.root_id, ct.depth, ct.child_id;

SELECT dblink_connect(
    'proj_link', 
    'host=localhost dbname=NOD_B user=postgres password=Bobo1999@'
);


-- Join with Harvest data for rollup computation
WITH RECURSIVE CropTree AS (
    SELECT child_id, child_id as root_id, 0 as depth
    FROM Crop_Hierarchy WHERE parent_id IS NULL
    UNION ALL
    SELECT ch.child_id, ct.root_id, ct.depth + 1
    FROM Crop_Hierarchy ch
    JOIN CropTree ct ON ch.parent_id = ct.child_id
)
SELECT 
    ct.root_id as crop_category,
    COUNT(DISTINCT h.harvest_id) as harvest_count,
    COALESCE(SUM(h.yield_kg), 0) as total_yield
FROM CropTree ct
LEFT JOIN Harvest_A h ON ct.child_id = h.crop_id
GROUP BY ct.root_id
ORDER BY ct.root_id;


-- B9: Mini-Knowledge Base with Transitive Inference (≤10 facts)

-- Create triple table for knowledge base
CREATE TABLE Agricultural_Triple (
    s VARCHAR(64),  -- Subject
    p VARCHAR(64),  -- Predicate
    o VARCHAR(64)   -- Object
);

-- Insert 8-10 domain facts
INSERT INTO Agricultural_Triple VALUES
('Maize', 'isA', 'Cereal'),
('Wheat', 'isA', 'Cereal'),
('Rice', 'isA', 'Cereal'),
('Beans', 'isA', 'Legume'),
('Cereal', 'isA', 'Crop'),
('Legume', 'isA', 'Crop'),
('Crop', 'requires', 'Water'),
('Crop', 'requires', 'Sunlight'),
('Maize', 'growing_season', 'Rainy'),
('Wheat', 'growing_season', 'Cool');

-- Recursive inference query for transitive isA relationships
WITH RECURSIVE TypeInference AS (
    -- Base case: Direct isA relationships
    SELECT 
        s as entity,
        o as direct_type,
        o as inferred_type,
        1 as depth,
        s || '->' || o as inference_path
    FROM Agricultural_Triple 
    WHERE p = 'isA'
    
    UNION ALL
    
    -- Recursive case: Transitive isA
    SELECT 
        ti.entity,
        ti.direct_type,
        at.o as inferred_type,
        ti.depth + 1,
        ti.inference_path || '->' || at.o
    FROM TypeInference ti
    JOIN Agricultural_Triple at ON ti.inferred_type = at.s AND at.p = 'isA'
    WHERE ti.depth < 5  -- Prevent infinite recursion
)
SELECT 
    entity,
    direct_type,
    inferred_type,
    depth,
    inference_path
FROM TypeInference
ORDER BY entity, depth;

-- Apply labels to base records

WITH RECURSIVE TypeInference AS (
    SELECT s AS entity, o AS inferred_type, 1 AS depth
    FROM Agricultural_Triple 
    WHERE p = 'isA'

    UNION ALL

    SELECT ti.entity, at.o AS inferred_type, ti.depth + 1
    FROM TypeInference ti
    JOIN Agricultural_Triple at 
      ON ti.inferred_type = at.s 
     AND at.p = 'isA'
    WHERE ti.depth < 5
)
SELECT DISTINCT
    c.crop_name AS base_record,
    ti.inferred_type AS label,
    ti.depth AS inference_depth
FROM dblink('host=localhost port=5432 dbname=NOD_B user=postgres password=Bobo1999@',
            'SELECT crop_id, crop_name FROM Crop') 
     AS c(crop_id INTEGER, crop_name VARCHAR(100))
JOIN TypeInference ti 
  ON c.crop_name = ti.entity
ORDER BY c.crop_name, ti.depth
LIMIT 10;


-- B10: Business Limit Alert (Function + Trigger)


-- Create business limits table
CREATE TABLE BUSINESS_LIMITS (
    rule_key VARCHAR(64) PRIMARY KEY,
    threshold DECIMAL(15,2),
    active CHAR(1) CHECK (active IN ('Y', 'N'))
);

-- Seed exactly one active rule
INSERT INTO BUSINESS_LIMITS VALUES 
('MAX_YIELD_PER_CROP', 10000.00, 'Y');

-- Create violation check function
CREATE OR REPLACE FUNCTION fn_should_alert(
    p_crop_id INTEGER,
    p_additional_yield DECIMAL(10,2)
) RETURNS INTEGER AS $$
DECLARE
    current_total DECIMAL(15,2);
    max_threshold DECIMAL(15,2);
BEGIN
    -- Get current total yield for the crop
    SELECT COALESCE(SUM(yield_kg), 0) INTO current_total
    FROM Harvest_A 
    WHERE crop_id = p_crop_id;
    
    -- Get threshold from business rules
    SELECT threshold INTO max_threshold
    FROM BUSINESS_LIMITS 
    WHERE rule_key = 'MAX_YIELD_PER_CROP' AND active = 'Y';
    
    -- Check if adding new yield would exceed threshold
    IF (current_total + p_additional_yield) > max_threshold THEN
        RETURN 1; -- Violation
    ELSE
        RETURN 0; -- No violation
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function
CREATE OR REPLACE FUNCTION check_yield_limit()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if new yield would violate business rule
    IF fn_should_alert(NEW.crop_id, NEW.yield_kg) = 1 THEN
        RAISE EXCEPTION 'Business rule violation: Crop % would exceed maximum yield threshold', NEW.crop_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER yield_limit_trigger
    BEFORE INSERT OR UPDATE ON Harvest_A
    FOR EACH ROW
    EXECUTE FUNCTION check_yield_limit();

-- Demonstrate 2 failing and 2 passing DML cases
DO $$
BEGIN
    RAISE NOTICE 'Testing Business Limit Alert...';
    
    -- Passing DML 1: Within limits
    BEGIN
        INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
        VALUES (17, 115, '2024-03-25', 500.00);
        RAISE NOTICE 'PASS: Insert within limits succeeded';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'UNEXPECTED FAIL: %', SQLERRM;
    END;
    
    -- Passing DML 2: Within limits  
    BEGIN
        INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
        VALUES (18, 116, '2024-03-26', 1000.00);
        RAISE NOTICE 'PASS: Insert within limits succeeded';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'UNEXPECTED FAIL: %', SQLERRM;
    END;
    
    -- Failing DML 1: Would exceed limit
    BEGIN
        INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
        VALUES (2, 117, '2024-03-27', 8000.00); -- Crop 2 already has some yield
        RAISE NOTICE 'UNEXPECTED PASS: Should have failed';
        ROLLBACK;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'EXPECTED FAIL: %', SQLERRM;
    END;
    
    -- Failing DML 2: Would exceed limit
    BEGIN
        INSERT INTO Harvest_A (crop_id, field_id, harvest_date, yield_kg) 
        VALUES (4, 118, '2024-03-28', 9000.00); -- Crop 4 already has some yield
        RAISE NOTICE 'UNEXPECTED PASS: Should have failed';
        ROLLBACK;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'EXPECTED FAIL: %', SQLERRM;
    END;
    
    COMMIT;
END $$;

-- Verify committed data and row budget
SELECT COUNT(*) as total_rows 
FROM (
    SELECT * FROM Harvest_A
    UNION ALL
    SELECT * 
    FROM dblink(
        'host=localhost port=5432 dbname=NOD_B user=postgres password=Bobo1999@',
        'SELECT * FROM Harvest_B'
    ) AS remote_harvest(
        harvest_id INTEGER,
        crop_id INTEGER,
        field_id INTEGER,
        harvest_date DATE,
        yield_kg DECIMAL(10,2),
        fragment_flag INTEGER
    )
) all_data;


-- Show current yields per crop to verify rule compliance
SELECT crop_id, SUM(yield_kg) as total_yield
FROM Harvest_A 
GROUP BY crop_id 
ORDER BY crop_id;

