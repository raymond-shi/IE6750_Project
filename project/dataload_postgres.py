##"Scale the overall output of powerplant for its consumption based on polulation based on zipcode , corelation technique"

import psycopg2
from psycopg2 import sql
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()

# PostgreSQL connection setup
db_config = {
    'host': 'localhost',
    'database': 'power_grid_db',
    'user': 'scott',
    'password': 'tiger123'
}

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Global dictionaries to store locations and city sizes
locations = {}
city_sizes = {}


def categorize_city_size(population):
    if population < 100000:
        return "small"
    elif population < 1000000:
        return "medium"
    else:
        return "large"

def generate_power_plants(num_plants):
    plants = []
    for _ in range(num_plants):
        city = fake.city()
        population = random.randint(10000, 5000000)
        plants.append((
            fake.company() + " Power Plant",
            random.uniform(100, 1000),  # capacity in MW
            city
        ))
        locations[city] = fake.state()
        city_sizes[city] = categorize_city_size(population)
    
    insert_query = sql.SQL("INSERT INTO Power_Plants (plant_name, capacity, location) VALUES (%s, %s, %s)")
    cursor.executemany(insert_query, plants)
    conn.commit()

def generate_transmission_lines(num_lines):
    cursor.execute("SELECT plant_id FROM Power_Plants")
    plant_ids = [row[0] for row in cursor.fetchall()]
    
    lines = [(fake.word() + " Line",
              random.choice([69, 138, 230, 345, 500]),  # voltage in kV
              random.uniform(10, 300),  # length in km
              random.choice(plant_ids)) for _ in range(num_lines)]
    
    insert_query = sql.SQL("INSERT INTO Transmission_Lines (line_name, voltage, length, plant_id) VALUES (%s, %s, %s, %s)")
    cursor.executemany(insert_query, lines)
    conn.commit()

def generate_substations(num_substations):
    substations = []
    for _ in range(num_substations):
        city = fake.city()
        population = random.randint(10000, 5000000)
        substations.append((
            fake.word() + " Substation",
            random.uniform(50, 500),  # capacity in MVA
            city
        ))
        locations[city] = fake.state()
        city_sizes[city] = categorize_city_size(population)
    
    insert_query = sql.SQL("INSERT INTO Substations (substation_name, capacity, location) VALUES (%s, %s, %s)")
    cursor.executemany(insert_query, substations)
    conn.commit()

def link_transmission_substations():
    cursor.execute("SELECT line_id FROM Transmission_Lines")
    line_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT substation_id FROM Substations")
    substation_ids = [row[0] for row in cursor.fetchall()]
    
    links = [(line_id, random.choice(substation_ids)) 
             for line_id in line_ids 
             for _ in range(random.randint(1, 3))]  # Each line connects to 1-3 substations
    
    insert_query = sql.SQL("INSERT INTO Transmission_Substation (line_id, substation_id) VALUES (%s, %s)")
    cursor.executemany(insert_query, links)
    conn.commit()

def generate_distribution_networks(num_networks):
    cursor.execute("SELECT substation_id, location FROM Substations")
    substation_data = cursor.fetchall()
    
    networks = []
    for _ in range(num_networks):
        substation_id, substation_location = random.choice(substation_data)
        networks.append((
            fake.word() + " Network",
            random.choice([4.16, 13.8, 34.5]),  # voltage in kV
            substation_id,
            substation_location  # Add location to network data
        ))
    
    insert_query = sql.SQL("INSERT INTO Distribution_Networks (network_name, voltage, substation_id, location) VALUES (%s, %s, %s, %s)")
    cursor.executemany(insert_query, networks)
    conn.commit()

def generate_customers(num_customers):
    cursor.execute("SELECT network_id, location FROM Distribution_Networks")
    network_data = cursor.fetchall()
    
    # Calculate weights based on city sizes
    weights = []
    for _, location in network_data:
        if city_sizes[location] == "small":
            weights.append(1)
        elif city_sizes[location] == "medium":
            weights.append(5)
        else:  # large
            weights.append(10)
    
    customers = []
    for _ in range(num_customers):
        network_id, network_location = random.choices(network_data, weights=weights, k=1)[0]
        state = locations.get(network_location, fake.state())
        customers.append((
            fake.name(),
            fake.street_address() + ", " + network_location + ", " + state + " " + fake.zipcode(),
            network_id
        ))
    
    insert_query = sql.SQL("INSERT INTO Customers (customer_name, address, network_id) VALUES (%s, %s, %s)")
    cursor.executemany(insert_query, customers)
    conn.commit()

def generate_meters(num_meters):
    cursor.execute("SELECT customer_id FROM Customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    meters = [(random.choice(['Smart', 'Analog', 'Digital']),
               fake.date_between(start_date='-5y', end_date='today'),
               customer_id) for customer_id in customer_ids[:num_meters]]  # Ensure unique customer_id
    
    insert_query = sql.SQL("INSERT INTO Meters (meter_type, installation_date, customer_id) VALUES (%s, %s, %s)")
    cursor.executemany(insert_query, meters)
    conn.commit()

def generate_energy_consumption(start_date, end_date):
    cursor.execute("""
        SELECT m.meter_id, dn.location 
        FROM Meters m
        JOIN Customers c ON m.customer_id = c.customer_id
        JOIN Distribution_Networks dn ON c.network_id = dn.network_id
    """)
    meter_data = cursor.fetchall()
    
    current_date = start_date
    while current_date <= end_date:
        consumptions = []
        for meter_id, location in meter_data:
            base_consumption = random.uniform(10, 100)  # Base consumption in kWh
            if city_sizes[location] == "small":
                consumption = base_consumption * random.uniform(0.8, 1.2)
            elif city_sizes[location] == "medium":
                consumption = base_consumption * random.uniform(1.2, 1.6)
            else:  # large
                consumption = base_consumption * random.uniform(1.6, 2.0)
            
            consumptions.append((
                meter_id,
                current_date,
                consumption
            ))
        
        insert_query = sql.SQL("INSERT INTO Energy_Consumption (meter_id, reading_date, consumption) VALUES (%s, %s, %s)")
        cursor.executemany(insert_query, consumptions)
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
                bills.append((customer_id,
                              current_date,
                              consumption * random.uniform(0.10, 0.15),  # amount in currency
                              consumption_id))
        
        insert_query = sql.SQL("INSERT INTO Billing (customer_id, billing_date, amount, consumption_id) VALUES (%s, %s, %s, %s)")
        cursor.executemany(insert_query, bills)
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
            maintenance = (current_date,
                           fake.sentence(),
                           random.uniform(1000, 10000))  # cost
            cursor.execute("INSERT INTO Maintenance (maintenance_date, description, cost) VALUES (%s, %s, %s) RETURNING maintenance_id", maintenance)
            maintenance_id = cursor.fetchone()[0]
            
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
            
            outage = (start_time,
                      end_time,
                      fake.sentence(),
                      asset_id,
                      asset_type)
            cursor.execute("INSERT INTO Outages (start_time, end_time, description, asset_id, asset_type) VALUES (%s, %s, %s, %s, %s) RETURNING outage_id", outage)
            outage_id = cursor.fetchone()[0]
            
            # Assign affected customers
            cursor.execute("SELECT customer_id FROM Customers ORDER BY RANDOM() LIMIT %s", (random.randint(1, 1000),))
            affected_customers = cursor.fetchall()
            customer_outages = [(outage_id, customer[0]) for customer in affected_customers]
            cursor.executemany("INSERT INTO Customer_Outage (outage_id, customer_id) VALUES (%s, %s)", customer_outages)
        
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