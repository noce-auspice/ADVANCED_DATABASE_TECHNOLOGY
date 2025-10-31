
-- 4. DDL for Worker
CREATE TABLE Worker (
    WorkerID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    Role VARCHAR(50) NOT NULL,
    Contact VARCHAR(50),
    DailyWage DECIMAL(8,2) CHECK (DailyWage >= 0)
);

-- 5. DDL for Harvest
CREATE TABLE Harvest (
    HarvestID SERIAL PRIMARY KEY,
    CropID INT NOT NULL,
    WorkerID INT NOT NULL,
    QuantityKG DECIMAL(10,2) CHECK (QuantityKG >= 0),
    DateCollected DATE NOT NULL,
    Grade VARCHAR(10),
    Buyer VARCHAR(50),
    FOREIGN KEY (WorkerID) REFERENCES Worker(WorkerID)
);

-- 6. DDL for Sale
CREATE TABLE Sale (
    SaleID SERIAL PRIMARY KEY,
    HarvestID INT NOT NULL,
    Buyer VARCHAR(50) NOT NULL,
    QuantitySold DECIMAL(10,2) CHECK (QuantitySold >= 0),
    PricePerKG DECIMAL(10,2) CHECK (PricePerKG >= 0),
    SaleDate DATE NOT NULL,
    FOREIGN KEY (HarvestID) REFERENCES Harvest(HarvestID)
        ON DELETE CASCADE
);



-- Insert Workers
INSERT INTO Worker (FullName, Role, Contact, DailyWage)
VALUES
('Alice Uwimana', 'Harvester', '0788000001', 3000),
('John Nkurunziza', 'Planter', '0788000002', 3500),
('Claudine Uwera', 'Supervisor', '0788000003', 4000),
('Eric Ndayisenga', 'Weeder', '0788000004', 2500),
('Diane Mukamana', 'Sprayer', '0788000005', 2800);

-- Insert Harvests
INSERT INTO Harvest (CropID, WorkerID, QuantityKG, DateCollected, Grade, Buyer)
VALUES
(1, 1, 5000, '2025-05-21', 'A', 'AgroCo Ltd'),
(2, 2, 3500, '2025-06-16', 'B', 'FarmLink'),
(3, 3, 0, '2025-07-20', 'A', NULL),
(4, 4, 4200, '2025-09-30', 'A', 'GreenMart'),
(5, 5, 3000, '2025-08-26', 'B', 'BioFarm');

-- Insert Sales
INSERT INTO Sale (HarvestID, Buyer, QuantitySold, PricePerKG, SaleDate)
VALUES
(1, 'AgroCo Ltd', 5000, 500, '2025-05-25'),
(2, 'FarmLink', 3500, 400, '2025-06-18'),
(4, 'GreenMart', 4200, 450, '2025-10-01'),
(5, 'BioFarm', 3000, 380, '2025-08-30');


