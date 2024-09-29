#!/usr/bin/env python
# coding: utf-8

# In[109]:


import random
from datetime import datetime, timedelta
import numpy as np
from faker import Faker
import pandas as pd

fake = Faker()

# Constants
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2024, 12, 31)
NUM_POWER_PLANTS = 50
NUM_TRANSMISSION_LINES = 200
NUM_SUBSTATIONS = 500
NUM_DISTRIBUTION_NETWORKS = 1000
NUM_CUSTOMERS = 1000000

# Lists of major US cities and their approximate populations
CITIES = [
    ("New York City", 8336817), ("Los Angeles", 3898747), ("Chicago", 2746388),
    ("Houston", 2304580), ("Phoenix", 1608139), ("Philadelphia", 1603797),
    ("San Antonio", 1434625), ("San Diego", 1386932), ("Dallas", 1304379),
    ("San Jose", 1013240), ("Austin", 961855), ("Jacksonville", 911507),
    ("Fort Worth", 909585), ("Columbus", 898553), ("San Francisco", 873965),
    ("Charlotte", 885708), ("Indianapolis", 876384), ("Seattle", 753675),
    ("Denver", 727211), ("Washington", 689545), ("Boston", 675647),
    ("El Paso", 681728), ("Detroit", 639111), ("Nashville", 689447),
    ("Portland", 641162), ("Memphis", 633104), ("Oklahoma City", 649021),
    ("Las Vegas", 651319), ("Louisville", 633045), ("Baltimore", 585708),
    ("Milwaukee", 577222), ("Albuquerque", 564559), ("Tucson", 548073),
    ("Fresno", 542107), ("Sacramento", 513624), ("Mesa", 504258),
    ("Kansas City", 508090), ("Atlanta", 498715), ("Long Beach", 466742),
    ("Omaha", 486051), ("Raleigh", 467665), ("Colorado Springs", 478221),
    ("Miami", 442241), ("Virginia Beach", 459470), ("Oakland", 440646),
    ("Minneapolis", 429606), ("Tulsa", 413066), ("Arlington", 398112),
    ("New Orleans", 383997), ("Wichita", 389255)
]

# Events that might affect energy consumption
EVENTS = [
    ("2020-03-15", "2020-06-30", "COVID-19 Lockdowns", -0.2),
    ("2021-02-13", "2021-02-17", "Texas Winter Storm", 0.5),
    ("2021-06-15", "2021-09-15", "Summer Heatwave", 0.3),
    ("2022-06-01", "2022-08-31", "Energy Price Spike", -0.1),
    ("2023-01-01", "2023-12-31", "Economic Recession", -0.15),
    ("2024-06-01", "2024-08-31", "Olympic Games", 0.2)
]




# In[179]:


def generate_asset(asset_id, asset_type):
    return (asset_id, asset_type)

def generate_power_plant(plant_id):
    plant_types = ["Coal", "Natural Gas", "Nuclear", "Hydroelectric", "Solar", "Wind"]
    plant_type = random.choice(plant_types)
    capacity = random.uniform(100, 2000)  # MW
    city, _ = random.choice(CITIES)
    return (plant_id, f"{city} {plant_type} Plant", capacity, city, plant_id)

def generate_transmission_line(line_id, power_plants):
    voltages = [110, 220, 345, 500, 765]  # kV
    plant = random.choice(power_plants)
    return (line_id, f"Line {line_id}", random.choice(voltages), random.uniform(50, 500), plant[0], line_id)

def generate_substation(substation_id):
    city, _ = random.choice(CITIES)
    return (substation_id, f"{city} Substation {substation_id}", random.uniform(100, 1000), city, substation_id)

def generate_distribution_network(network_id, substations):
    substation = random.choice(substations)
    return (network_id, f"Network {network_id}", 11.0, substation[0], network_id)

def generate_customer(customer_id, networks):
    network = random.choice(networks)
    city = next(sub for sub in substations if sub[0] == network[3])[2]
    return (customer_id, fake.name(), fake.address().replace('\n', ', ') + f", {city}", network[0])

def generate_meter(meter_id, customer_id):
    meter_types = ["Smart", "Analog", "Digital"]
    return (meter_id, random.choice(meter_types), fake.date_between(start_date=START_DATE, end_date=END_DATE), customer_id)

def generate_consumption(consumption_id, meter_id, date, base_consumption):
    # Strong seasonal variation
    month = date.month
    day_of_year = date.timetuple().tm_yday
    # print(month)
    # print(day_of_year)
    
    # Summer peak (July) and winter peak (January) with smoother transitions
    seasonal_factor = 1 + 0.5 * (np.sin((day_of_year - 15) * 2 * np.pi / 365) + 
                                 0.5 * np.sin((day_of_year - 15) * 4 * np.pi / 365))
    
    # Temperature variation (approximate, you may want to use actual temperature data for more accuracy)
    temp_variation = random.gauss(0, 0.1)  # Random normal distribution
    seasonal_factor += temp_variation
    
    # Weekly pattern (higher consumption on weekdays)
    weekday_factor = 1.1 if date.weekday() < 5 else 0.9
    
    # Apply random daily variation
    daily_variation = random.uniform(0.9, 1.1)
    
    # Apply event effects
    event_effect = 1
    for event_start, event_end, _, effect in EVENTS:
        # print(datetime.strptime(event_end, "%Y-%m-%d"))
        # print(event_start)
        # print(type(event_start))
        # print(event_end)
        # print(type(event_end))
        if datetime.strptime(event_start, "%Y-%m-%d").date() <= date <= datetime.strptime(event_end, "%Y-%m-%d").date():
            event_effect += effect
        # if event_start <= date <= event_end:
        #     event_effect += effect
    
    # Calculate final consumption
    consumption = base_consumption * seasonal_factor * weekday_factor * daily_variation * event_effect
    
    return (consumption_id, meter_id, date, max(0, consumption))

def generate_billing(bill_id, customer_id, consumption_id, consumption, date):
    rate = random.uniform(0.1, 0.2)  # $/kWh
    amount = consumption * rate
    return (bill_id, customer_id, date, amount, consumption_id)

def generate_outage(outage_id, assets):
    asset = random.choice(assets)
    start_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
    end_time = start_time + timedelta(hours=random.randint(1, 24))
    return (outage_id, start_time, end_time, f"Outage on {asset[1]}", asset[0])

def generate_customers(num_customers, distribution_networks):
    customers = []
    # Create a dictionary to map cities to their distribution networks
    city_to_networks = {}
    for network in distribution_networks:
        city = next(sub for sub in substations if sub[0] == network[3])[3]
        if city not in city_to_networks:
            city_to_networks[city] = []
        city_to_networks[city].append(network)
    
    # Generate customers
    for i in range(1, num_customers + 1):
        # Choose a city based on population
        city, population = random.choices(CITIES, weights=[city[1] for city in CITIES])[0]
        
        # If the chosen city has no networks, choose the nearest city that does
        while city not in city_to_networks:
            city, population = random.choice(CITIES)
        
        # Choose a network in the selected city
        network = random.choice(city_to_networks[city])
        
        customers.append((i, fake.name(), fake.address().replace('\n', ', ') + f", {city}", network[0]))
        #print(customers)
    
    return customers

# Generate customers

# In[9]:


# Generate data
assets = [generate_asset(i, "Power Plant") for i in range(1, NUM_POWER_PLANTS + 1)]
assets += [generate_asset(i, "Transmission Line") for i in range(NUM_POWER_PLANTS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1)]
assets += [generate_asset(i, "Substation") for i in range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1)]
assets += [generate_asset(i, "Distribution Network") for i in range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + NUM_DISTRIBUTION_NETWORKS + 1)]

power_plants = [generate_power_plant(i) for i in range(1, NUM_POWER_PLANTS + 1)]
transmission_lines = [generate_transmission_line(i, power_plants) for i in range(NUM_POWER_PLANTS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1)]
substations = [generate_substation(i) for i in range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1)]
distribution_networks = [generate_distribution_network(i, substations) for i in range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + NUM_DISTRIBUTION_NETWORKS + 1)]



# In[105]:


for network in distribution_networks:
    city = next(sub for sub in substations if sub[0] == network[3])[3]

city, population = random.choices(CITIES, weights=[city[1] for city in CITIES])[0]
print(city, population)


print(city, population)



# Generate customers with realistic distribution
customers = []
# for i in range(1, NUM_CUSTOMERS + 1):
#     network = random.choices(distribution_networks, weights=[city[1] for city in CITIES[:len(distribution_networks)]])[0]
#     customers.append(generate_customer(i, [network]))

customers = generate_customers(NUM_CUSTOMERS, distribution_networks)



import pandas as pd
cust_df=pd.DataFrame(customers)


# In[117]:


meters = [generate_meter(i, customer[0]) for i, customer in enumerate(customers, start=1)]


# In[209]:


len(meters)


# In[205]:


#meters = [generate_meter(i, customer[0]) for i, customer in enumerate(customers, start=1)]

# Generate consumption and billing data
consumption_data = []
billing_data = []
consumption_id = 1
bill_id = 1

for meter in meters[:100]:
    base_consumption = random.uniform(200, 1000)  # kWh per month
    date = meter[2]  # Installation date
    #print(date)
    #print(END_DATE.date())
    while date <= END_DATE.date():
        consumption = generate_consumption(consumption_id, meter[0], date, base_consumption / 30)
        consumption_data.append(consumption)
        
        if date.day == 1:  # Generate monthly bills
            total_consumption = sum(c[3] for c in consumption_data if c[1] == meter[0] and c[2].month == date.month and c[2].year == date.year)
            billing_data.append(generate_billing(bill_id, meter[3], consumption_id, total_consumption, date))
            bill_id += 1
        
        consumption_id += 1
        date += timedelta(days=1)




outages = [generate_outage(i, assets) for i in range(1, 1001)]  # Generate 1000 outages


# In[201]:


# Create DataFrames
df_assets = pd.DataFrame(assets, columns=['asset_id', 'asset_type'])
df_power_plants = pd.DataFrame(power_plants, columns=['plant_id', 'plant_name', 'capacity', 'location', 'asset_id'])
df_transmission_lines = pd.DataFrame(transmission_lines, columns=['line_id', 'line_name', 'voltage', 'length', 'plant_id', 'asset_id'])
df_substations = pd.DataFrame(substations, columns=['substation_id', 'substation_name', 'capacity', 'location', 'asset_id'])
df_distribution_networks = pd.DataFrame(distribution_networks, columns=['network_id', 'network_name', 'voltage', 'substation_id', 'asset_id'])
df_customers = pd.DataFrame(customers, columns=['customer_id', 'customer_name', 'address', 'network_id'])
df_meters = pd.DataFrame(meters, columns=['meter_id', 'meter_type', 'installation_date', 'customer_id'])
# df_consumption = pd.DataFrame(consumption_data, columns=['consumption_id', 'meter_id', 'reading_date', 'consumption'])
# df_billing = pd.DataFrame(billing_data, columns=['bill_id', 'customer_id', 'billing_date', 'amount', 'consumption_id'])
# df_outages = pd.DataFrame(outages, columns=['outage_id', 'start_time', 'end_time', 'description', 'asset_id'])


# In[211]:


def save_to_csv(df, filename):
    df.to_csv(filename, index=False)

# Save all DataFrames to CSV files
save_to_csv(df_assets, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/assets.csv')
save_to_csv(df_power_plants, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/power_plants.csv')
save_to_csv(df_transmission_lines, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/transmission_lines.csv')
save_to_csv(df_substations, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/substations.csv')
save_to_csv(df_distribution_networks, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/distribution_networks.csv')
save_to_csv(df_customers, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/customers.csv')
save_to_csv(df_meters, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/meters.csv')
save_to_csv(df_consumption, 'consumption.csv')
save_to_csv(df_billing, 'billing.csv')
save_to_csv(df_outages, 'outages.csv')

print("Data generation complete. CSV files have been created.")

