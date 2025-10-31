-- 1. Rules (Declarative Constraints): Safe Prescriptions

-- Schema setup
CREATE SCHEMA IF NOT EXISTS healthnet;
SET search_path TO healthnet;

-- Prerequisite table
CREATE TABLE patient (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Fixed PATIENT_MED table with proper constraints
CREATE TABLE patient_med (
    patient_med_id SERIAL PRIMARY KEY,
    patient_id INT NOT NULL REFERENCES patient(id),
    med_name VARCHAR(80) NOT NULL,
    dose_mg NUMERIC(6,2) CHECK (dose_mg >= 0),
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    CONSTRAINT ck_rx_dates CHECK (start_dt <= end_dt)
);

-- Passing INSERTs
INSERT INTO patient (name) VALUES ('Alice');
INSERT INTO patient (name) VALUES ('Bob');

INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (1, 'Amoxicillin', 250.00, '2025-01-01', '2025-01-10');

INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (2, 'Ibuprofen', 100.00, '2025-02-01', '2025-02-05');

-- Failing INSERTs (should be rejected)
-- Negative dose
INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (1, 'Paracetamol', -50.00, '2025-01-01', '2025-01-05');

-- Inverted dates
INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (1, 'Ceftriaxone', 250.00, '2025-02-05', '2025-01-30');



-- 2️. Active Databases (E–C–A Trigger): Bill Totals That Stay Correct

-- Prerequisites
CREATE TABLE bill (
    id SERIAL PRIMARY KEY,
    total NUMERIC(12,2) DEFAULT 0
);

CREATE TABLE bill_item (
    bill_id INT REFERENCES bill(id),
    amount NUMERIC(12,2),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE bill_audit (
    bill_id INT,
    old_total NUMERIC(12,2),
    new_total NUMERIC(12,2),
    changed_at TIMESTAMP DEFAULT now()
);

-- Statement-level trigger using a temporary table
CREATE OR REPLACE FUNCTION update_bill_totals()
RETURNS TRIGGER AS $$
BEGIN
    -- Update total for all affected bills
    UPDATE bill b
    SET total = COALESCE((
        SELECT SUM(amount) FROM bill_item i WHERE i.bill_id = b.id
    ), 0)
    WHERE b.id IN (SELECT DISTINCT bill_id FROM bill_item);

    -- Insert audit record
    INSERT INTO bill_audit (bill_id, old_total, new_total, changed_at)
    SELECT b.id, NULL, b.total, now()
    FROM bill b;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_bill_total_stmt
AFTER INSERT OR UPDATE OR DELETE ON bill_item
FOR EACH STATEMENT
EXECUTE FUNCTION update_bill_totals();

-- Test
INSERT INTO bill VALUES (1,0);
INSERT INTO bill_item VALUES (1,100,now()), (1,200,now());
UPDATE bill_item SET amount=300 WHERE amount=100;
DELETE FROM bill_item WHERE amount=200;
SELECT * FROM bill;
SELECT * FROM bill_audit;


-- 3️. Deductive Databases (Recursive WITH): Referral/Supervision Chain


-- Prerequisite
CREATE TABLE staff_supervisor (
    employee VARCHAR(50),
    supervisor VARCHAR(50)
);

INSERT INTO staff_supervisor VALUES
('Alice','Bob'),
('Bob','Carol'),
('Carol','Diana'),
('Eve','Bob'),
('Frank','Eve');

-- Recursive query
WITH RECURSIVE supers(emp, sup, hops, path) AS (
  SELECT employee, supervisor, 1, employee::TEXT || '>' || supervisor
  FROM staff_supervisor
  UNION ALL
  SELECT s.employee, t.sup, hops + 1, path || '>' || t.sup
  FROM staff_supervisor s
  JOIN supers t ON s.supervisor = t.emp
  WHERE POSITION(t.sup IN path) = 0 -- prevent cycles
)
SELECT emp, sup AS top_supervisor, MAX(hops) AS hops
FROM supers
GROUP BY emp, sup
ORDER BY emp;



-- 4️. Knowledge Bases (Triples & Ontology): Infectious-Disease Roll-Up

-- Prerequisite
CREATE TABLE triple (
    s VARCHAR(100),
    p VARCHAR(50),
    o VARCHAR(100)
);

INSERT INTO triple VALUES
('Patient1', 'hasDiagnosis', 'Influenza'),
('Patient2', 'hasDiagnosis', 'Malaria'),
('Patient3', 'hasDiagnosis', 'Fracture'),
('Influenza', 'isA', 'ViralInfection'),
('ViralInfection', 'isA', 'InfectiousDisease'),
('Malaria', 'isA', 'InfectiousDisease'),
('Fracture', 'isA', 'Injury'),
('Injury', 'isA', 'NonInfectiousCondition');

-- Compute transitive closure of isA
WITH RECURSIVE isa(child, ancestor) AS (
  SELECT s, o FROM triple WHERE p = 'isA'
  UNION
  SELECT t.s, i.ancestor
  FROM triple t
  JOIN isa i ON t.o = i.child
  WHERE t.p = 'isA'
),
infectious_patients AS (
  SELECT DISTINCT t.s
  FROM triple t
  JOIN isa i ON t.o = i.child
  WHERE t.p = 'hasDiagnosis'
    AND i.ancestor = 'InfectiousDisease'
)
SELECT s AS patient_id FROM infectious_patients;


-- 5️. Spatial Databases (PostGIS): Radius & Nearest-3


-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE clinic (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    geom GEOGRAPHY(Point, 4326)
);

-- Insert sample clinics
INSERT INTO clinic (name, geom)
VALUES
('Kigali Clinic', ST_GeogFromText('SRID=4326;POINT(30.0610 -1.9575)')),
('Remera Hospital', ST_GeogFromText('SRID=4326;POINT(30.0650 -1.9580)')),
('Gisozi Center', ST_GeogFromText('SRID=4326;POINT(30.0700 -1.9600)')),
('Nyamirambo Health', ST_GeogFromText('SRID=4326;POINT(30.0500 -1.9700)'));

-- Ambulance location
WITH params AS (
  SELECT ST_GeogFromText('SRID=4326;POINT(30.0600 -1.9570)') AS amb_point
)
-- 1) Within 1 km
SELECT c.id, c.name
FROM clinic c, params p
WHERE ST_DWithin(c.geom, p.amb_point, 1000);

-- 2) Nearest 3 clinics
SELECT c.id, c.name,
       ROUND(ST_Distance(c.geom, p.amb_point)/1000, 3) AS distance_km
FROM clinic c, params p
ORDER BY distance_km
LIMIT 3;


























































