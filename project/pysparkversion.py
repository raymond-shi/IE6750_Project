from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, rand, explode, sequence, to_date, datediff, expr, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, TimestampType
import random
from datetime import datetime, timedelta
import numpy as np
from faker import Faker

# Initialize Spark session
spark = SparkSession.builder.appName("EnergyConsumption").getOrCreate()

# Initialize Faker
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

# UDFs
@udf(returnType=StructType([
    StructField("asset_id", IntegerType(), True),
    StructField("asset_type", StringType(), True)
]))
def generate_asset(asset_id, asset_type):
    return (asset_id, asset_type)

@udf(returnType=StructType([
    StructField("plant_id", IntegerType(), True),
    StructField("plant_name", StringType(), True),
    StructField("capacity", FloatType(), True),
    StructField("city", StringType(), True),
    StructField("asset_id", IntegerType(), True)
]))
def generate_power_plant(plant_id):
    plant_types = ["Coal", "Natural Gas", "Nuclear", "Hydroelectric", "Solar", "Wind"]
    plant_type = random.choice(plant_types)
    capacity = random.uniform(100, 2000)  # MW
    city, _ = random.choice(CITIES)
    return (plant_id, f"{city} {plant_type} Plant", capacity, city, plant_id)

@udf(returnType=StructType([
    StructField("line_id", IntegerType(), True),
    StructField("line_name", StringType(), True),
    StructField("voltage", IntegerType(), True),
    StructField("length", FloatType(), True),
    StructField("plant_id", IntegerType(), True),
    StructField("asset_id", IntegerType(), True)
]))
def generate_transmission_line(line_id, power_plants):
    voltages = [110, 220, 345, 500, 765]  # kV
    plant = random.choice(power_plants)
    return (line_id, f"Line {line_id}", random.choice(voltages), random.uniform(50, 500), plant[0], line_id)

@udf(returnType=StructType([
    StructField("substation_id", IntegerType(), True),
    StructField("substation_name", StringType(), True),
    StructField("capacity", FloatType(), True),
    StructField("city", StringType(), True),
    StructField("asset_id", IntegerType(), True)
]))
def generate_substation(substation_id):
    city, _ = random.choice(CITIES)
    return (substation_id, f"{city} Substation {substation_id}", random.uniform(100, 1000), city, substation_id)

@udf(returnType=StructType([
    StructField("network_id", IntegerType(), True),
    StructField("network_name", StringType(), True),
    StructField("voltage", FloatType(), True),
    StructField("substation_id", IntegerType(), True),
    StructField("asset_id", IntegerType(), True)
]))
def generate_distribution_network(network_id, substations):
    substation = random.choice(substations)
    return (network_id, f"Network {network_id}", 11.0, substation[0], network_id)

@udf(returnType=StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("network_id", IntegerType(), True)
]))
def generate_customer(customer_id, networks):
    network = random.choice(networks)
    city = next(sub for sub in substations if sub[0] == network[3])[2]
    return (customer_id, fake.name(), fake.address().replace('\n', ', ') + f", {city}", network[0])

@udf(returnType=StructType([
    StructField("meter_id", IntegerType(), True),
    StructField("meter_type", StringType(), True),
    StructField("installation_date", DateType(), True),
    StructField("customer_id", IntegerType(), True)
]))
def generate_meter(meter_id, customer_id):
    meter_types = ["Smart", "Analog", "Digital"]
    return (meter_id, random.choice(meter_types), fake.date_between(start_date=START_DATE, end_date=END_DATE), customer_id)

@udf(returnType=StructType([
    StructField("consumption_id", IntegerType(), True),
    StructField("meter_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("consumption", FloatType(), True)
]))
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

@udf(returnType=StructType([
    StructField("bill_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", FloatType(), True),
    StructField("consumption_id", IntegerType(), True)
]))
def generate_billing(bill_id, customer_id, consumption_id, consumption, date):
    rate = random.uniform(0.1, 0.2)  # $/kWh
    amount = consumption * rate
    return (bill_id, customer_id, date, amount, consumption_id)

@udf(returnType=StructType([
    StructField("outage_id", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("asset_id", IntegerType(), True)
]))
def generate_outage(outage_id, assets):
    asset = random.choice(assets)
    start_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
    end_time = start_time + timedelta(hours=random.randint(1, 24))
    return (outage_id, start_time, end_time, f"Outage on {asset[1]}", asset[0])

# Generate data
assets_df = spark.range(1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + NUM_DISTRIBUTION_NETWORKS + 1) \
    .withColumn("asset_type", when(col("id") <= NUM_POWER_PLANTS, "Power Plant")
                .when(col("id") <= NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES, "Transmission Line")
                .when(col("id") <= NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS, "Substation")
                .otherwise("Distribution Network")) \
    .withColumn("asset", generate_asset("id", "asset_type")) \
    .select("asset.*")

power_plants_df = spark.range(1, NUM_POWER_PLANTS + 1) \
    .withColumn("plant", generate_power_plant("id")) \
    .select("plant.*")

transmission_lines_df = spark.range(NUM_POWER_PLANTS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1) \
    .withColumn("line", generate_transmission_line("id", power_plants_df.collect())) \
    .select("line.*")

substations_df = spark.range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1) \
    .withColumn("substation", generate_substation("id")) \
    .select("substation.*")

distribution_networks_df = spark.range(NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + 1, NUM_POWER_PLANTS + NUM_TRANSMISSION_LINES + NUM_SUBSTATIONS + NUM_DISTRIBUTION_NETWORKS + 1) \
    .withColumn("network", generate_distribution_network("id", substations_df.collect())) \
    .select("network.*")

customers_df = spark.range(1, NUM_CUSTOMERS + 1) \
    .withColumn("customer", generate_customer("id", distribution_networks_df.collect())) \
    .select("customer.*")

meters_df = spark.range(1, NUM_CUSTOMERS + 1) \
    .withColumn("meter", generate_meter("id", "id")) \
    .select("meter.*")

# Generate consumption and billing data
date_range = spark.sql(f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) as date")

consumption_df = meters_df.crossJoin(date_range) \
    .withColumn("base_consumption", rand() * 800 + 200) \
    .withColumn("consumption", generate_consumption(
        monotonically_increasing_id(),
        "meter_id",
        "date",
        "base_consumption" / 30
    )) \
    .select("consumption.*")

billing_df = consumption_df \
    .where(expr("day(date) = 1")) \
    .groupBy("meter_id", "date") \
    .agg({"consumption": "sum"}) \
    .withColumnRenamed("sum(consumption)", "total_consumption") \
    .withColumn("billing", generate_billing(
        monotonically_increasing_id(),
        "meter_id",
        monotonically_increasing_id(),
        "total_consumption",
        "date"
    )) \
    .select("billing.*")

outages_df = spark.range(1, 1001) \
    .withColumn("outage", generate_outage("id", assets_df.collect())) \
    .select("outage.*")

# Save DataFrames to CSV files
def save_to_csv(df, filename):
    df.write.csv(filename, header=True, mode="overwrite")

save_to_csv(assets_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/assets')
save_to_csv(power_plants_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/power_plants')
save_to_csv(transmission_lines_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/transmission_lines')
save_to_csv(substations_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/substations')
save_to_csv(distribution_networks_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/distribution_networks')
save_to_csv(customers_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/customers')
save_to_csv(meters_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/meters')
save_to_csv(consumption_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/consumption')
save_to_csv(billing_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/billing')
save_to_csv(outages_df, '/Users/subhasishbhaumik/Documents/neu/IE6750/project_data/outages')

print("Data generation complete. CSV files have been created.")

# Stop Spark session
spark.stop()