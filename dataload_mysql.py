import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()

# MySQL connection setup
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'power_grid_db'
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

def generate_power_plants(num_plants):
    plants = []
    for _ in range(num_plants):
        plant = (
            fake.company() + " Power Plant",
            random.uniform(100, 1000),  # capacity in MW
            fake.city()
        )
        plants.append(plant)
    
    cursor.executemany("INSERT INTO Power_Plants (plant_name, capacity, location) VALUES (%s, %s, %s)", plants)
    conn.commit()

def generate_transmission_lines(num_lines):
    cursor.execute("SELECT plant_id FROM Power_Plants")
    plant_ids = [row[0] for row in cursor.fetchall()]
    
    lines = []
    for _ in range(num_lines):
        line = (
            fake.word() + " Line",
            random.choice([69, 138, 230, 345, 500]),  # voltage in kV
            random.uniform(10, 300),  # length in km
            random.choice(plant_ids)
        )
        lines.append(line)
    
    cursor.executemany("INSERT INTO Transmission_Lines (line_name, voltage, length, plant_id) VALUES (%s, %s, %s, %s)", lines)
    conn.commit()

def generate_substations(num_substations):
    substations = []
    for _ in range(num_substations):
        substation = (
            fake.word() + " Substation",
            random.uniform(50, 500),  # capacity in MVA
            fake.city()
        )
        substations.append(substation)
    
    cursor.executemany("INSERT INTO Substations (substation_name, capacity, location) VALUES (%s, %s, %s)", substations)
    conn.commit()

def link_transmission_substations():
    cursor.execute("SELECT line_id FROM Transmission_Lines")
    line_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT substation_id FROM Substations")
    substation_ids = [row[0] for row in cursor.fetchall()]
    
    links = []
    for line_id in line_ids:
        for _ in range(random.randint(1, 3)):  # Each line connects to 1-3 substations
            links.append((line_id, random.choice(substation_ids)))
    
    cursor.executemany("INSERT INTO Transmission_Substation (line_id, substation_id) VALUES (%s, %s)", links)
    conn.commit()

def generate_distribution_networks(num_networks):
    cursor.execute("SELECT substation_id FROM Substations")
    substation_ids = [row[0] for row in cursor.fetchall()]
    
    networks = []
    for _ in range(num_networks):
        network = (
            fake.word() + " Network",
            random.choice([4.16, 13.8, 34.5]),  # voltage in kV
            random.choice(substation_ids)
        )
        networks.append(network)
    
    cursor.executemany("INSERT INTO Distribution_Networks (network_name, voltage, substation_id) VALUES (%s, %s, %s)", networks)
    conn.commit()

def generate_customers(num_customers):
    cursor.execute("SELECT network_id FROM Distribution_Networks")
    network_ids = [row[0] for row in cursor.fetchall()]
    
    customers = []
    for _ in range(num_customers):
        customer = (
            fake.name(),
            fake.address(),
            random.choice(network_ids)
        )
        customers.append(customer)
    
    cursor.executemany("INSERT INTO Customers (customer_name, address, network_id) VALUES (%s, %s, %s)", customers)
    conn.commit()

def generate_meters(num_meters):
    cursor.execute("SELECT customer_id FROM Customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    meters = []
    for customer_id in customer_ids[:num_meters]:  # Ensure unique customer_id
        meter = (
            random.choice(['Smart', 'Analog', 'Digital']),
            fake.date_between(start_date='-5y', end_date='today'),
            customer_id
        )
        meters.append(meter)
    
    cursor.executemany("INSERT INTO Meters (meter_type, installation_date, customer_id) VALUES (%s, %s, %s)", meters)
    conn.commit()

def generate_energy_consumption(start_date, end_date):
    cursor.execute("SELECT meter_id FROM Meters")
    meter_ids = [row[0] for row in cursor.fetchall()]
    
    current_date = start_date
    while current_date <= end_date:
        consumptions = []
        for meter_id in meter_ids:
            consumption = (
                meter_id,
                current_date,
                random.uniform(10, 100)  # consumption in kWh
            )
            consumptions.append(consumption)
        
        cursor.executemany("INSERT INTO Energy_Consumption (meter_id, reading_date, consumption) VALUES (%s, %s, %s)", consumptions)
        conn.commit()
        current_date += timedelta(days=1)

def generate_billing(start_date, end_date):
    cursor.execute("SELECT customer_id FROM Customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    current_date = start_date
    while current_date <= end_date:
        bills = []
        for customer_id in customer_ids:
            # Get the latest consumption for this customer
            cursor.execute("""
                SELECT ec.consumption_id, ec.consumption 
                FROM Energy_Consumption ec
                JOIN Meters m ON ec.meter_id = m.meter_id
                WHERE m.customer_id = %s AND ec.reading_date = %s
            """, (customer_id, current_date))
            result = cursor.fetchone()
            if result:
                consumption_id, consumption = result
                bill = (
                    customer_id,
                    current_date,
                    consumption * random.uniform(0.10, 0.15),  # amount in currency
                    consumption_id
                )
                bills.append(bill)
        
        cursor.executemany("INSERT INTO Billing (customer_id, billing_date, amount, consumption_id) VALUES (%s, %s, %s, %s)", bills)
        conn.commit()
        current_date += timedelta(days=30)  # Monthly billing

def generate_maintenance(start_date, end_date):
    cursor.execute("SELECT plant_id FROM Power_Plants")
    plant_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT line_id FROM Transmission_Lines")
    line_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT substation_id FROM Substations")
    substation_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT network_id FROM Distribution_Networks")
    network_ids = [row[0] for row in cursor.fetchall()]
    
    current_date = start_date
    while current_date <= end_date:
        if random.random() < 0.1:  # 10% chance of maintenance on any given day
            maintenance = (
                current_date,
                fake.sentence(),
                random.uniform(1000, 10000)  # cost
            )
            cursor.execute("INSERT INTO Maintenance (maintenance_date, description, cost) VALUES (%s, %s, %s)", maintenance)
            maintenance_id = cursor.lastrowid
            
            # Randomly choose an asset for maintenance
            asset_type = random.choice(['plant', 'line', 'substation', 'network'])
            if asset_type == 'plant':
                asset_id = random.choice(plant_ids)
            elif asset_type == 'line':
                asset_id = random.choice(line_ids)
            elif asset_type == 'substation':
                asset_id = random.choice(substation_ids)
            else:
                asset_id = random.choice(network_ids)
            
            cursor.execute("INSERT INTO Asset_Maintenance (maintenance_id, asset_id, asset_type) VALUES (%s, %s, %s)", 
                           (maintenance_id, asset_id, asset_type))
        
        conn.commit()
        current_date += timedelta(days=1)

def generate_outages(start_date, end_date):
    cursor.execute("SELECT plant_id FROM Power_Plants")
    plant_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT line_id FROM Transmission_Lines")
    line_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT substation_id FROM Substations")
    substation_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT network_id FROM Distribution_Networks")
    network_ids = [row[0] for row in cursor.fetchall()]
    
    current_date = start_date
    while current_date <= end_date:
        if random.random() < 0.05:  # 5% chance of outage on any given day
            asset_type = random.choice(['plant', 'line', 'substation', 'network'])
            if asset_type == 'plant':
                asset_id = random.choice(plant_ids)
            elif asset_type == 'line':
                asset_id = random.choice(line_ids)
            elif asset_type == 'substation':
                asset_id = random.choice(substation_ids)
            else:
                asset_id = random.choice(network_ids)
            
            start_time = current_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            duration = timedelta(hours=random.uniform(0.5, 8))  # Outage duration between 30 minutes and 8 hours
            end_time = start_time + duration
            
            outage = (
                start_time,
                end_time,
                fake.sentence(),
                asset_id,
                asset_type
            )
            cursor.execute("INSERT INTO Outages (start_time, end_time, description, asset_id, asset_type) VALUES (%s, %s, %s, %s, %s)", outage)
            outage_id = cursor.lastrowid
            
            # Assign affected customers
            cursor.execute("SELECT customer_id FROM Customers LIMIT %s", (random.randint(1, 1000),))
            affected_customers = cursor.fetchall()
            for customer in affected_customers:
                cursor.execute("INSERT INTO Customer_Outage (outage_id, customer_id) VALUES (%s, %s)", (outage_id, customer[0]))
        
        conn.commit()
        current_date += timedelta(days=1)

# Main execution
if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    generate_power_plants(10)
    generate_transmission_lines(20)
    generate_substations(30)
    link_transmission_substations()
    generate_distribution_networks(50)
    generate_customers(10000)
    generate_meters(10000)
    generate_energy_consumption(start_date, end_date)
    generate_billing(start_date, end_date)
    generate_maintenance(start_date, end_date)
    generate_outages(start_date, end_date)
    
    print("Data generation complete!")

    conn.close()