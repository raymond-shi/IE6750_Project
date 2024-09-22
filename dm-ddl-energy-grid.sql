-- Dimension Tables

CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE Dim_Customer (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(100),
    address VARCHAR(200),
    network_id INT
);

CREATE TABLE Dim_PowerPlant (
    plant_key INT PRIMARY KEY,
    plant_id INT,
    plant_name VARCHAR(100),
    capacity FLOAT,
    location VARCHAR(100)
);

CREATE TABLE Dim_Meter (
    meter_key INT PRIMARY KEY,
    meter_id INT,
    meter_type VARCHAR(50),
    installation_date DATE
);

CREATE TABLE Dim_Asset (
    asset_key INT PRIMARY KEY,
    asset_id INT,
    asset_type ENUM('plant', 'line', 'substation', 'network'),
    asset_name VARCHAR(100)
);

-- Fact Tables

CREATE TABLE Fact_EnergyConsumption (
    consumption_key INT PRIMARY KEY,
    date_key INT,
    customer_key INT,
    meter_key INT,
    consumption FLOAT,
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
    FOREIGN KEY (meter_key) REFERENCES Dim_Meter(meter_key)
);

CREATE TABLE Fact_Billing (
    bill_key INT PRIMARY KEY,
    date_key INT,
    customer_key INT,
    consumption_key INT,
    amount DECIMAL(10, 2),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
    FOREIGN KEY (consumption_key) REFERENCES Fact_EnergyConsumption(consumption_key)
);

CREATE TABLE Fact_Maintenance (
    maintenance_key INT PRIMARY KEY,
    date_key INT,
    asset_key INT,
    cost DECIMAL(10, 2),
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (asset_key) REFERENCES Dim_Asset(asset_key)
);

CREATE TABLE Fact_Outage (
    outage_key INT PRIMARY KEY,
    start_date_key INT,
    end_date_key INT,
    asset_key INT,
    duration_hours FLOAT,
    num_customers_affected INT,
    FOREIGN KEY (start_date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (end_date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (asset_key) REFERENCES Dim_Asset(asset_key)
);

CREATE TABLE Fact_PowerGeneration (
    generation_key INT PRIMARY KEY,
    date_key INT,
    plant_key INT,
    energy_generated FLOAT,
    FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    FOREIGN KEY (plant_key) REFERENCES Dim_PowerPlant(plant_key)
);