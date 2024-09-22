-- Create Power Plants table
CREATE TABLE Power_Plants (
    plant_id INT PRIMARY KEY ,
    plant_name VARCHAR(100) NOT NULL,
    capacity FLOAT NOT NULL,
    location VARCHAR(100)
);

-- Create Transmission Lines table
CREATE TABLE Transmission_Lines (
    line_id INT PRIMARY KEY ,
    line_name VARCHAR(100) NOT NULL,
    voltage FLOAT NOT NULL,
    length FLOAT NOT NULL,
    plant_id INT,
    FOREIGN KEY (plant_id) REFERENCES Power_Plants(plant_id)
);

-- Create Substations table
CREATE TABLE Substations (
    substation_id INT PRIMARY KEY ,
    substation_name VARCHAR(100) NOT NULL,
    capacity FLOAT NOT NULL,
    location VARCHAR(100)
);

-- Create Transmission_Substation junction table
CREATE TABLE Transmission_Substation (
    line_id INT,
    substation_id INT,
    PRIMARY KEY (line_id, substation_id),
    FOREIGN KEY (line_id) REFERENCES Transmission_Lines(line_id),
    FOREIGN KEY (substation_id) REFERENCES Substations(substation_id)
);

-- Create Distribution Networks table
CREATE TABLE Distribution_Networks (
    network_id INT PRIMARY KEY ,
    network_name VARCHAR(100) NOT NULL,
    voltage FLOAT NOT NULL,
    substation_id INT,
    FOREIGN KEY (substation_id) REFERENCES Substations(substation_id)
);

-- Create Customers table
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY ,
    customer_name VARCHAR(100) NOT NULL,
    address VARCHAR(200),
    network_id INT,
    FOREIGN KEY (network_id) REFERENCES Distribution_Networks(network_id)
);

-- Create Meters table
CREATE TABLE Meters (
    meter_id INT PRIMARY KEY ,
    meter_type VARCHAR(50) NOT NULL,
    installation_date DATE,
    customer_id INT UNIQUE,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

-- Create Energy Consumption table
CREATE TABLE Energy_Consumption (
    consumption_id INT PRIMARY KEY ,
    meter_id INT,
    reading_date DATE NOT NULL,
    consumption FLOAT NOT NULL,
    FOREIGN KEY (meter_id) REFERENCES Meters(meter_id)
);

-- Create Billing table
CREATE TABLE Billing (
    bill_id INT PRIMARY KEY ,
    customer_id INT,
    billing_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    consumption_id INT UNIQUE,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (consumption_id) REFERENCES Energy_Consumption(consumption_id)
);

-- Create Maintenance table
CREATE TABLE Maintenance (
    maintenance_id INT PRIMARY KEY ,
    maintenance_date DATE NOT NULL,
    description TEXT,
    cost DECIMAL(10, 2)
);

-- Create Asset_Maintenance junction table
CREATE TABLE Asset_Maintenance (
    maintenance_id INT,
    asset_id INT,
    asset_type varchar(50) NOT NULL,
    PRIMARY KEY (maintenance_id, asset_id, asset_type),
    FOREIGN KEY (maintenance_id) REFERENCES Maintenance(maintenance_id)
);

-- Create Outages table
CREATE TABLE Outages (
    outage_id INT PRIMARY KEY ,
    start_time timestamp NOT NULL,
    end_time timestamp,
    description TEXT,
    asset_id INT
);

-- Create Customer_Outage junction table
CREATE TABLE Customer_Outage (
    outage_id INT,
    customer_id INT,
    PRIMARY KEY (outage_id, customer_id),
    FOREIGN KEY (outage_id) REFERENCES Outages(outage_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);