import os
from datetime import datetime, timedelta
from faker import Faker
from clickhouse_driver import Client
from dotenv import load_dotenv
import random

load_dotenv()  # Load environment variables from .env

fake = Faker()

# ClickHouse connection configuration
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'mysql_user',
    'password': os.getenv("PASSWORD"),
    'database': 'default'
}

# Initialize ClickHouse client
ch_client = Client(**CLICKHOUSE_CONFIG)

def create_tables():
    # Drop existing tables if needed (uncomment to use)
    # ch_client.execute('DROP TABLE IF EXISTS Fact_Sales')
    # ch_client.execute('DROP TABLE IF EXISTS Dim_Time')
    # ... repeat for other tables ...

    # Create dimension tables
    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Time (
            date_id         Int32,
            date            Date,
            month           Int8,
            year            Int16
        ) ENGINE = MergeTree()
        ORDER BY date_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Product (
            product_id      Int32,
            product_name    String,
            category        String,
            price           Decimal(10,2),
            supplier_id     Int32
        ) ENGINE = MergeTree()
        ORDER BY product_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Region (
            region_id       Int32,
            region_name     String,
            country         String,
            sales_manager   String
        ) ENGINE = MergeTree()
        ORDER BY region_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Sales_Channel (
            channel_id      Int32,
            channel_name    String,
            platform        String
        ) ENGINE = MergeTree()
        ORDER BY channel_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Customer (
            customer_id         Int32,
            name                String,
            region_id           Int32,
            age_group           Nullable(String),
            gender              Nullable(String),
            membership_status   Nullable(String),
            average_balance     Nullable(Decimal(15,2)),
            average_income      Nullable(Decimal(15,2)),
            business_risk_class Nullable(String),
            is_pep              Bool,
            account_balance     Nullable(Decimal(15,2)),
            is_cash_intensive   Bool,
            tpr_threshold_exceeded Bool,
            transacts_hr_jurisdictions Bool,
            preferred_channel   Nullable(String),
            interests           Array(String),
            occupation          Nullable(String),
            lifecycle_stage     Nullable(String),
            churn_risk_score    Nullable(Decimal(5,2)),
            predicted_clv       Nullable(Decimal(12,2)),
            consent_marketing   Bool,
            consent_data_share  Bool,
            data_deletion_date  Nullable(Date)
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    ''')

    # Create fact tables
    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Fact_Sales (
            sale_id          Int32,
            date_id          Int32,
            product_id       Int32,
            customer_id      Int32,
            region_id        Int32,
            channel_id       Int32,
            units_sold       Int32,
            revenue          Decimal(12,2),
            discount_amount  Decimal(10,2)
        ) ENGINE = MergeTree()
        ORDER BY sale_id
    ''')

def generate_dimension_data():
    # Generate Dim_Time (3 years of dates)
    dim_time = []
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2023, 12, 31)
    current_date = start_date
    date_id = 1
    while current_date <= end_date:
        dim_time.append({
            'date_id': date_id,
            'date': current_date.date(),
            'month': current_date.month,
            'year': current_date.year
        })
        current_date += timedelta(days=1)
        date_id += 1

    # Generate Dim_Region (50 regions)
    dim_regions = []
    for region_id in range(1, 51):
        dim_regions.append({
            'region_id': region_id,
            'region_name': fake.state(),
            'country': fake.country(),
            'sales_manager': fake.name()
        })

    # Generate Dim_Sales_Channel (5 channels)
    dim_sales_channels = [
        {'channel_id': 1, 'channel_name': 'Online Store', 'platform': 'Web'},
        {'channel_id': 2, 'channel_name': 'Retail Store', 'platform': 'In-Store'},
        {'channel_id': 3, 'channel_name': 'Wholesale', 'platform': 'B2B'},
        {'channel_id': 4, 'channel_name': 'Mobile App', 'platform': 'iOS/Android'},
        {'channel_id': 5, 'channel_name': 'Marketplace', 'platform': 'Amazon'}
    ]

    # Generate Dim_Product (200 products)
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Toys']
    dim_products = []
    for product_id in range(1, 201):
        dim_products.append({
            'product_id': product_id,
            'product_name': f"{fake.unique.word().capitalize()} {random.choice(['Pro', 'Deluxe', 'Basic'])}",
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 500.0), 2),
            'supplier_id': random.randint(1, 50)
        })

    # Generate Dim_Customer (5000 customers)
    age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
    dim_customers = []
    for customer_id in range(1, 5001):
        dim_customers.append({
            'customer_id': customer_id,
            'name': fake.name(),
            'region_id': random.randint(1, 50),
            'age_group': random.choice(age_groups),
            'gender': random.choice(['M', 'F', 'O']),
            'membership_status': random.choice(['Gold', 'Silver', 'Bronze', 'None']),
            'average_balance': round(random.uniform(1000, 100000), 2),
            'average_income': round(random.uniform(20000, 150000), 2),
            'business_risk_class': random.choice(['High Risk', 'Medium Risk', 'Low Risk', 'Not Classified']),
            'is_pep': random.choices([True, False], weights=[0.1, 0.9])[0],
            'account_balance': round(random.uniform(0, 50000), 2),
            'is_cash_intensive': random.choices([True, False], weights=[0.2, 0.8])[0],
            'tpr_threshold_exceeded': random.choices([True, False], weights=[0.3, 0.7])[0],
            'transacts_hr_jurisdictions': random.choices([True, False], weights=[0.1, 0.9])[0],
            'preferred_channel': random.choice(['Email', 'SMS', 'App Notification', 'Post']),
            'interests': random.sample(['Sports', 'Tech', 'Fashion', 'Books'], k=random.randint(1, 3)),
            'occupation': fake.job(),
            'lifecycle_stage': random.choice(['Prospect', 'First-Time', 'Regular', 'VIP']),
            'churn_risk_score': round(random.uniform(0, 5), 2),
            'predicted_clv': round(random.uniform(100, 10000), 2),
            'consent_marketing': random.choice([True, False]),
            'consent_data_share': random.choice([True, False]),
            'data_deletion_date': fake.date_this_decade() if random.random() < 0.2 else None
        })

    # Insert dimension data
    ch_client.execute('INSERT INTO Dim_Time VALUES', dim_time)
    ch_client.execute('INSERT INTO Dim_Region VALUES', dim_regions)
    ch_client.execute('INSERT INTO Dim_Sales_Channel VALUES', dim_sales_channels)
    ch_client.execute('INSERT INTO Dim_Product VALUES', dim_products)
    ch_client.execute('INSERT INTO Dim_Customer VALUES', dim_customers)

def generate_fact_sales(num_records=500000, batch_size=50000):
    # Get existing keys
    date_ids = [row[0] for row in ch_client.execute('SELECT date_id FROM Dim_Time')]
    product_ids = [row[0] for row in ch_client.execute('SELECT product_id FROM Dim_Product')]
    customer_ids = [row[0] for row in ch_client.execute('SELECT customer_id FROM Dim_Customer')]
    region_ids = [row[0] for row in ch_client.execute('SELECT region_id FROM Dim_Region')]
    channel_ids = [row[0] for row in ch_client.execute('SELECT channel_id FROM Dim_Sales_Channel')]

    # Generate in batches
    for i in range(0, num_records, batch_size):
        batch = []
        current_batch_size = min(batch_size, num_records - i)

        for sale_id in range(i + 1, i + current_batch_size + 1):
            date_id = random.choice(date_ids)
            product_id = random.choice(product_ids)
            customer_id = random.choice(customer_ids)
            region_id = random.choice(region_ids)
            channel_id = random.choice(channel_ids)
            units = random.randint(1, 10)

            # Get product price from dimension data
            price = ch_client.execute(
                'SELECT price FROM Dim_Product WHERE product_id = %(product_id)s',
                {'product_id': product_id}
            )[0][0]

            discount = round(random.uniform(0, float(price) * 0.2), 2)
            revenue = round(units * float(price) - discount, 2)

            batch.append({
                'sale_id': sale_id,
                'date_id': date_id,
                'product_id': product_id,
                'customer_id': customer_id,
                'region_id': region_id,
                'channel_id': channel_id,
                'units_sold': units,
                'revenue': revenue,
                'discount_amount': discount
            })

        # Insert batch
        ch_client.execute('INSERT INTO Fact_Sales VALUES', batch)
        print(f'Inserted {len(batch)} records (Total: {i + len(batch)}/{num_records})')

if __name__ == '__main__':
    create_tables()
    generate_dimension_data()
    generate_fact_sales()
    print("Data generation complete!")
