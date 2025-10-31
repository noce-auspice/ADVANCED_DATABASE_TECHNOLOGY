-- Task 1: Build all six tables with constraints
-- TASK 2: Apply CASCADE DELETE between Crop → Harvest and Harvest → Sale


-- Field table
CREATE TABLE Field (
    FieldID INT PRIMARY KEY,
    FieldName VARCHAR(100) NOT NULL,
    SizeHectares DECIMAL(10,2) NOT NULL CHECK (SizeHectares > 0),
    Location VARCHAR(100) NOT NULL,
    SoilType VARCHAR(50) NOT NULL
);

-- Crop table
CREATE TABLE Crop (
    CropID INT PRIMARY KEY,
    FieldID INT NOT NULL,
    CropName VARCHAR(100) NOT NULL,
    PlantingDate DATE NOT NULL,
    HarvestDate DATE,
    Status VARCHAR(20) DEFAULT 'Planted' CHECK (Status IN ('Planted', 'Growing', 'Ready', 'Harvested', 'Sold')),
    FOREIGN KEY (FieldID) REFERENCES Field(FieldID),
    CHECK (HarvestDate IS NULL OR HarvestDate >= PlantingDate)
);

-- Fertilizer table
CREATE TABLE Fertilizer (
    FertilizerID INT PRIMARY KEY,
    CropID INT NOT NULL,
    Name VARCHAR(100) NOT NULL,
    QuantityUsed DECIMAL(10,2) NOT NULL CHECK (QuantityUsed > 0),
    Cost DECIMAL(10,2) NOT NULL CHECK (Cost >= 0),
    DateApplied DATE NOT NULL,
    FOREIGN KEY (CropID) REFERENCES Crop(CropID)
);

-- Worker table
CREATE TABLE Worker (
    WorkerID INT PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    Role VARCHAR(50) NOT NULL,
    Contact VARCHAR(15) NOT NULL,
    DailyWage DECIMAL(10,2) NOT NULL CHECK (DailyWage >= 0)
);

-- Harvest table
CREATE TABLE Harvest (
    HarvestID INT PRIMARY KEY,
    CropID INT NOT NULL,
    WorkerID INT NOT NULL,
    QuantityKG DECIMAL(10,2) NOT NULL CHECK (QuantityKG > 0),
    DateCollected DATE NOT NULL,
    Grade VARCHAR(10) CHECK (Grade IN ('A', 'B', 'C', 'D')),
    Buyer VARCHAR(100),
    FOREIGN KEY (CropID) REFERENCES Crop(CropID) ON DELETE CASCADE,
    FOREIGN KEY (WorkerID) REFERENCES Worker(WorkerID)
);

-- Sale table
CREATE TABLE Sale (
    SaleID INT PRIMARY KEY,
    HarvestID INT NOT NULL,
    Buyer VARCHAR(100) NOT NULL,
    QuantitySold DECIMAL(10,2) NOT NULL CHECK (QuantitySold > 0),
    PricePerKG DECIMAL(10,2) NOT NULL CHECK (PricePerKG > 0),
    SaleDate DATE NOT NULL,
    FOREIGN KEY (HarvestID) REFERENCES Harvest(HarvestID) ON DELETE CASCADE
);


-- Task 3: Insert sample data


-- Insert 3 fields
INSERT INTO Field (FieldID, FieldName, SizeHectares, Location, SoilType) VALUES
(1, 'North Field', 5.0, 'Northern Section', 'Loamy'),
(2, 'South Field', 3.5, 'Southern Section', 'Clay'),
(3, 'East Field', 4.2, 'Eastern Section', 'Sandy Loam');

SELECT * FROM Field

-- Insert 5 crops
INSERT INTO Crop (CropID, FieldID, CropName, PlantingDate, HarvestDate, Status) VALUES
(1, 1, 'Maize', '2024-03-15', '2024-07-20', 'Harvested'),
(2, 1, 'Beans', '2024-04-01', '2024-06-30', 'Sold'),
(3, 2, 'Wheat', '2024-03-20', '2024-07-15', 'Harvested'),
(4, 2, 'Tomatoes', '2024-04-10', '2024-07-05', 'Ready'),
(5, 3, 'Potatoes', '2024-03-25', '2024-07-25', 'Growing');

SELECT * FROM Crop

-- Insert 5 workers
INSERT INTO Worker (WorkerID, FullName, Role, Contact, DailyWage) VALUES
(1, 'John Kamau', 'Harvester', '0712345678', 1200.00),
(2, 'Mary Wanjiku', 'Supervisor', '0723456789', 2000.00),
(3, 'Peter Omondi', 'Planter', '0734567890', 1000.00),
(4, 'Grace Achieng', 'Harvester', '0745678901', 1200.00),
(5, 'James Mwangi', 'Irrigator', '0756789012', 1100.00);

SELECT * FROM Worker

-- Insert sample fertilizers
INSERT INTO Fertilizer (FertilizerID, CropID, Name, QuantityUsed, Cost, DateApplied) VALUES
(1, 1, 'NPK 17:17:17', 50.0, 2500.00, '2024-04-01'),
(2, 1, 'Urea', 25.0, 1500.00, '2024-05-15'),
(3, 2, 'DAP', 30.0, 1800.00, '2024-04-10'),
(4, 3, 'NPK 20:20:20', 40.0, 2200.00, '2024-04-05'),
(5, 4, 'Organic Compost', 60.0, 3000.00, '2024-04-20');

SELECT * FROM Fertilizer

-- Insert sample harvests
INSERT INTO Harvest (HarvestID, CropID, WorkerID, QuantityKG, DateCollected, Grade, Buyer) VALUES
(1, 1, 1, 1500.5, '2024-07-20', 'A', 'GreenMart Ltd'),
(2, 2, 4, 800.75, '2024-06-30', 'B', 'FreshProduce Co'),
(3, 3, 1, 1200.25, '2024-07-15', 'A', 'GrainCorp'),
(4, 4, 4, 600.0, '2024-07-05', 'B', 'Local Market'),
(5, 1, 3, 500.0, '2024-07-25', 'A', 'Export Quality');

SELECT * FROM Harvest

-- Insert sample sales
INSERT INTO Sale (SaleID, HarvestID, Buyer, QuantitySold, PricePerKG, SaleDate) VALUES
(1, 1, 'GreenMart Ltd', 1500.5, 45.00, '2024-07-21'),
(2, 2, 'FreshProduce Co', 800.75, 60.00, '2024-07-01'),
(3, 3, 'GrainCorp', 1200.25, 50.00, '2024-07-16'),
(4, 4, 'Local Market', 600.0, 40.00, '2024-07-06'),
(5, 5, 'Export Quality', 500.0, 70.00, '2024-07-26');

SELECT * FROM Sale


-- Task 4: Retrieve harvest yield per field

SELECT 
    f.FieldName,
    f.Location,
    SUM(h.QuantityKG) AS TotalHarvestKG,
    COUNT(DISTINCT c.CropID) AS NumberOfCrops
FROM Field f
JOIN Crop c ON f.FieldID = c.FieldID
JOIN Harvest h ON c.CropID = h.CropID
GROUP BY f.FieldID, f.FieldName, f.Location
ORDER BY TotalHarvestKG DESC;



-- Task 5: Update crop status after sale completion

-- Update crop status to 'Sold' when sales are completed
UPDATE Crop 
SET Status = 'Sold' 
WHERE CropID IN (
    SELECT DISTINCT c.CropID
    FROM Crop c
    JOIN Harvest h ON c.CropID = h.CropID
    JOIN Sale s ON h.HarvestID = s.HarvestID
    WHERE c.Status != 'Sold'
);

-- Verify the update
SELECT CropID, CropName, Status FROM Crop;


-- Task 6: Identify the most profitable crop of the season

SELECT 
    c.CropName,
    f.FieldName,
    SUM(s.QuantitySold * s.PricePerKG) AS TotalRevenue,
    SUM(fert.TotalFertilizerCost) AS TotalFertilizerCost,
    (SUM(s.QuantitySold * s.PricePerKG) - SUM(fert.TotalFertilizerCost)) AS NetProfit
FROM Crop c
JOIN Field f ON c.FieldID = f.FieldID
JOIN Harvest h ON c.CropID = h.CropID
JOIN Sale s ON h.HarvestID = s.HarvestID
JOIN (
    SELECT CropID, SUM(Cost) AS TotalFertilizerCost
    FROM Fertilizer
    GROUP BY CropID
) fert ON c.CropID = fert.CropID
GROUP BY c.CropID, c.CropName, f.FieldName
ORDER BY NetProfit DESC
LIMIT 1;



-- Task 7: Create a view summarizing total fertilizer cost per crop

CREATE VIEW CropFertilizerCostSummary AS
SELECT 
    c.CropID,
    c.CropName,
    f.FieldName,
    COUNT(fert.FertilizerID) AS NumberOfApplications,
    SUM(fert.QuantityUsed) AS TotalQuantityUsed,
    SUM(fert.Cost) AS TotalFertilizerCost,
    AVG(fert.Cost) AS AverageCostPerApplication
FROM Crop c
JOIN Field f ON c.FieldID = f.FieldID
LEFT JOIN Fertilizer fert ON c.CropID = fert.CropID
GROUP BY c.CropID, c.CropName, f.FieldName;

-- Query the view
SELECT * FROM CropFertilizerCostSummary ORDER BY TotalFertilizerCost DESC;


-- Task 8: Implement a trigger preventing fertilizer application before planting date

CREATE OR REPLACE FUNCTION check_fertilizer_date()
RETURNS TRIGGER AS $$
DECLARE
    plant_date DATE;
BEGIN
    -- Get the planting date for the crop
    SELECT PlantingDate INTO plant_date
    FROM Crop
    WHERE CropID = NEW.CropID;
    
    -- Check if fertilizer application date is before planting date
    IF NEW.DateApplied < plant_date THEN
        RAISE EXCEPTION 'Fertilizer cannot be applied before planting date';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER check_fertilizer_date_trigger
BEFORE INSERT ON Fertilizer
FOR EACH ROW
EXECUTE FUNCTION check_fertilizer_date();

-- Test the trigger (this should fail)
INSERT INTO Fertilizer (FertilizerID, CropID, Name, QuantityUsed, Cost, DateApplied) 
VALUES (6, 1, 'Test Fertilizer', 10.0, 500.00, '2024-03-01');









