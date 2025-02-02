import os
from datetime import datetime, timedelta
from faker import Faker
from clickhouse_driver import Client
from dotenv import load_dotenv
import random
import uuid

load_dotenv()

fake = Faker()

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'mysql_user',
    'password': os.getenv("PASSWORD"),
    'database': 'default'
}

ch_client = Client(**CLICKHOUSE_CONFIG)

def create_tables():
    # Get list of all tables
    tables = ch_client.execute(
                "SELECT name FROM system.tables WHERE database = 'default'"
            )

    # Drop each table
    for table in tables:
        table_name = table[0]  # Extract table name
        ch_client.execute(f"DROP TABLE IF EXISTS default.{table_name}")

    print("All tables dropped successfully!")

    print("Creating new tables...")
    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Time (
            date_id         Int32,
            date            Date,
            month           Int8,
            year           Int16
        ) ENGINE = MergeTree()
        ORDER BY date_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Product (
            product_id      Int32,
            product_name    String,
            category        String,
            price          Float64,
            supplier_id     Int32
        ) ENGINE = MergeTree()
        ORDER BY product_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Region (
            region_id       Int32,
            region_name     String,
            country        String,
            sales_manager   String
        ) ENGINE = MergeTree()
        ORDER BY region_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Sales_Channel (
            channel_id      Int32,
            channel_name    String,
            platform       String
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
            average_balance     Nullable(Float64),
            average_income      Nullable(Float64),
            business_risk_class Nullable(String),
            is_pep              Bool,
            account_balance     Nullable(Float64),
            is_cash_intensive   Bool,
            tpr_threshold_exceeded Bool,
            transacts_hr_jurisdictions Bool,
            preferred_channel   Nullable(String),
            interests           Array(String),
            occupation          Nullable(String),
            lifecycle_stage     Nullable(String),
            churn_risk_score    Nullable(Float64),
            predicted_clv       Nullable(Float64),
            consent_marketing   Bool,
            consent_data_share  Bool,
            data_deletion_date  Nullable(Date),
            risk_profile        String
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Dim_Loan (
            loan_id             Int32,
            customer_id         Int32,
            loan_amount         Float64,
            interest_rate       Float64,
            term_months         Int32,
            start_date          Date,
            end_date            Date,
            loan_status         String,
            loan_type           String,
            risk_rating         String,
            collateral_value    Float64,
            application_channel String,
            application_date    Date,
            last_payment_date   Nullable(Date),
            next_payment_due_date Nullable(Date),
            outstanding_balance Float64
        ) ENGINE = MergeTree()
        ORDER BY loan_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Fact_Sales (
            sale_id          Int32,
            date_id         Int32,
            product_id      Int32,
            customer_id     Int32,
            region_id       Int32,
            channel_id      Int32,
            units_sold      Int32,
            revenue         Float64,
            discount_amount Float64,
            processing_fees Float64,
            documentation_fees Float64,
            insurance_fees  Float64,
            customer_acquisition_cost Float64,
            emi_bounce_charges Float64,
            npa_loss_amount Float64,
            total_revenue   Float64,
            status          String
        ) ENGINE = MergeTree()
        ORDER BY sale_id
    ''')

    ch_client.execute('''
        CREATE TABLE IF NOT EXISTS Fact_Loan_Repayment (
            repayment_id    Int32,
            loan_id        Int32,
            customer_id    Int32,
            emi_number     Int32,
            due_date       Date,
            payment_date   Nullable(Date),
            emi_amount     Float64,
            principal_amount Float64,
            interest_amount Float64,
            penalties      Float64,
            payment_status String,
            payment_mode   String,
            pending_principal Float64,
            pending_interest Float64,
            days_overdue   Int32,
            bounce_reason  Nullable(String),
            collection_agent_id Nullable(Int32)
        ) ENGINE = MergeTree()
        ORDER BY (loan_id, emi_number)
    ''')

def generate_dimension_data():
    # Generate Dim_Time
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

    # Generate Dim_Region
    dim_regions = []
    for region_id in range(1, 51):
        dim_regions.append({
            'region_id': region_id,
            'region_name': fake.state(),
            'country': fake.country(),
            'sales_manager': fake.name()
        })

    # Generate Dim_Sales_Channel
    dim_sales_channels = [
        {'channel_id': 1, 'channel_name': 'Ads', 'platform': 'Online'},
        {'channel_id': 2, 'channel_name': 'Third Party NBFCs', 'platform': 'Partner'},
        {'channel_id': 3, 'channel_name': 'Agents', 'platform': 'Field'},
        {'channel_id': 4, 'channel_name': 'Branch', 'platform': 'In-Person'},
        {'channel_id': 5, 'channel_name': 'Telemarketing', 'platform': 'Phone'}
    ]

    # Generate Dim_Product with loan products
    categories = ['Auto Loan', 'Mortgage', 'Business Loan', 'Personal Loan', 'Credit Card']
    dim_products = []
    for product_id in range(1, 201):
        category = random.choices(categories, weights=[0.3, 0.25, 0.2, 0.15, 0.1], k=1)[0]
        if category == 'Auto Loan':
            product_name = f"{random.choice(['New', 'Used'])} Auto Loan {random.choice(['Standard', 'Premium'])}"
            price = round(random.uniform(100.0, 500.0), 2)
        elif category == 'Mortgage':
            product_name = f"{random.choice(['Fixed', 'Adjustable'])} Rate Mortgage {random.choice(['30-Year', '15-Year'])}"
            price = round(random.uniform(500.0, 2000.0), 2)
        elif category == 'Business Loan':
            product_name = f"Business Loan {random.choice(['Short-Term', 'Long-Term'])}"
            price = round(random.uniform(200.0, 1000.0), 2)
        elif category == 'Personal Loan':
            product_name = "Personal Loan"
            price = round(random.uniform(50.0, 300.0), 2)
        else:
            product_name = "Credit Card"
            price = round(random.uniform(0.0, 100.0), 2)

        dim_products.append({
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'price': price,
            'supplier_id': random.randint(1, 50)
        })

    # Generate Dim_Customer with risk profiles
    age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
    dim_customers = []
    for customer_id in range(1, 5001):
        risk_profile = random.choices(['Low', 'Medium', 'High'], weights=[0.7, 0.2, 0.1])[0]
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
            'data_deletion_date': fake.date_this_decade() if random.random() < 0.2 else None,
            'risk_profile': risk_profile
        })

    # Insert dimension data
    ch_client.execute('INSERT INTO Dim_Time VALUES', dim_time)
    ch_client.execute('INSERT INTO Dim_Region VALUES', dim_regions)
    ch_client.execute('INSERT INTO Dim_Sales_Channel VALUES', dim_sales_channels)
    ch_client.execute('INSERT INTO Dim_Product VALUES', dim_products)
    ch_client.execute('INSERT INTO Dim_Customer VALUES', dim_customers)
    print("Dimension data inserted.")

def generate_fact_sales(num_records=100000, batch_size=10000):
    # Preload customer risk profiles
    customer_risk = {row[0]: row[1] for row in ch_client.execute('SELECT customer_id, risk_profile FROM Dim_Customer')}

    # Get dimension keys
    date_ids = [row[0] for row in ch_client.execute('SELECT date_id FROM Dim_Time')]
    product_ids = [row[0] for row in ch_client.execute('SELECT product_id FROM Dim_Product')]
    customer_ids = list(customer_risk.keys())
    region_ids = [row[0] for row in ch_client.execute('SELECT region_id FROM Dim_Region')]
    channel_ids = [row[0] for row in ch_client.execute('SELECT channel_id FROM Dim_Sales_Channel')]

    for i in range(0, num_records, batch_size):
        batch = []
        current_batch_size = min(batch_size, num_records - i)

        for sale_id in range(i + 1, i + current_batch_size + 1):
            customer_id = random.choice(customer_ids)
            risk_profile = customer_risk[customer_id]

            # Determine approval status
            if risk_profile == 'High':
                approved = random.choices([True, False], weights=[0.2, 0.8])[0]
            elif risk_profile == 'Medium':
                approved = random.choices([True, False], weights=[0.6, 0.4])[0]
            else:
                approved = random.choices([True, False], weights=[0.9, 0.1])[0]
            status = 'Approved' if approved else 'Rejected'

            product_id = random.choice(product_ids)
            price = ch_client.execute(
                'SELECT price FROM Dim_Product WHERE product_id = %(product_id)s',
                {'product_id': product_id}
            )[0][0]

            units = 1
            if status == 'Approved':
                discount = round(random.uniform(0, price * 0.2), 2)
                base_revenue = round(price - discount, 2)

                # Additional fees and costs
                processing_fees = round(base_revenue * random.uniform(0.01, 0.02), 2)
                documentation_fees = round(random.uniform(500, 2000), 2)
                insurance_fees = round(base_revenue * random.uniform(0.005, 0.015), 2)
                customer_acquisition_cost = round(random.uniform(1000, 5000), 2)

                # EMI bounce charges and NPA losses based on risk profile
                if risk_profile == 'High':
                    emi_bounce_charges = round(random.uniform(500, 2000), 2)
                    npa_loss_amount = round(base_revenue * random.uniform(0.1, 0.3), 2) if random.random() < 0.2 else 0
                elif risk_profile == 'Medium':
                    emi_bounce_charges = round(random.uniform(200, 1000), 2)
                    npa_loss_amount = round(base_revenue * random.uniform(0.05, 0.15), 2) if random.random() < 0.1 else 0
                else:
                    emi_bounce_charges = round(random.uniform(0, 500), 2)
                    npa_loss_amount = round(base_revenue * random.uniform(0.02, 0.08), 2) if random.random() < 0.05 else 0

                total_revenue = base_revenue + processing_fees + documentation_fees + insurance_fees + emi_bounce_charges - npa_loss_amount
            else:
                base_revenue = discount = processing_fees = documentation_fees = insurance_fees = 0
                customer_acquisition_cost = round(random.uniform(500, 2000), 2)  # Cost still incurred for rejected applications
                emi_bounce_charges = npa_loss_amount = 0
                total_revenue = 0

            batch.append({
                'sale_id': sale_id,
                'date_id': random.choice(date_ids),
                'product_id': product_id,
                'customer_id': customer_id,
                'region_id': random.choice(region_ids),
                'channel_id': random.choice(channel_ids),
                'units_sold': units,
                'revenue': base_revenue,
                'discount_amount': discount,
                'processing_fees': processing_fees,
                'documentation_fees': documentation_fees,
                'insurance_fees': insurance_fees,
                'customer_acquisition_cost': customer_acquisition_cost,
                'emi_bounce_charges': emi_bounce_charges,
                'npa_loss_amount': npa_loss_amount,
                'total_revenue': total_revenue,
                'status': status
            })

        ch_client.execute('INSERT INTO Fact_Sales VALUES', batch)
        print(f'Inserted {len(batch)} sales records (Total: {i + len(batch)}/{num_records})')

def generate_loan_repayments():
    print("Generating loan repayment data...")

    # Get all active and delinquent loans
    loans = ch_client.execute('''
        SELECT loan_id, customer_id, loan_amount, interest_rate, term_months, start_date, loan_status, risk_rating
        FROM Dim_Loan
        WHERE loan_status IN ('Active', 'Delinquent', 'Defaulted')
    ''')

    repayments = []
    repayment_id = 1

    payment_modes = ['UPI', 'NEFT', 'Auto-Debit', 'Cash', 'Cheque']
    bounce_reasons = ['Insufficient Funds', 'Account Closed', 'Payment Stopped', 'Technical Error']

    for loan in loans:
        loan_id, customer_id, loan_amount, interest_rate, term_months, start_date, loan_status, risk_rating = loan

        # Calculate EMI
        monthly_rate = interest_rate / (12 * 100)
        emi_amount = round((loan_amount * monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1), 2)

        outstanding_principal = loan_amount

        for emi_number in range(1, term_months + 1):
            due_date = start_date + timedelta(days=30 * emi_number)

            # Skip future EMIs
            if due_date > datetime.now().date():
                continue

            interest_amount = round(outstanding_principal * monthly_rate, 2)
            principal_amount = round(min(emi_amount - interest_amount, outstanding_principal), 2)

            # Determine payment status and date based on loan status and risk
            if loan_status == 'Active':
                if risk_rating == 'High':
                    payment_status = random.choices(['Paid', 'Bounced', 'Partial'], weights=[0.7, 0.2, 0.1])[0]
                elif risk_rating == 'Medium':
                    payment_status = random.choices(['Paid', 'Bounced', 'Partial'], weights=[0.8, 0.15, 0.05])[0]
                else:
                    payment_status = random.choices(['Paid', 'Bounced', 'Partial'], weights=[0.9, 0.08, 0.02])[0]
            elif loan_status == 'Delinquent':
                payment_status = random.choices(['Overdue', 'Partial'], weights=[0.7, 0.3])[0]
            else:  # Defaulted
                payment_status = 'Defaulted'

            payment_date = None
            penalties = 0
            days_overdue = 0
            bounce_reason = None
            collection_agent_id = None

            if payment_status == 'Paid':
                payment_date = due_date + timedelta(days=random.randint(-5, 2))
                pending_principal = pending_interest = 0
            elif payment_status == 'Bounced':
                payment_date = due_date + timedelta(days=random.randint(1, 5))
                bounce_reason = random.choice(bounce_reasons)
                penalties = round(emi_amount * 0.02, 2)  # 2% penalty
                pending_principal = principal_amount
                pending_interest = interest_amount
            elif payment_status == 'Partial':
                payment_date = due_date + timedelta(days=random.randint(1, 10))
                partial_percent = random.uniform(0.4, 0.8)
                pending_principal = round(principal_amount * (1 - partial_percent), 2)
                pending_interest = round(interest_amount * (1 - partial_percent), 2)
                penalties = round(emi_amount * 0.01, 2)  # 1% penalty
            else:  # Overdue or Defaulted
                days_overdue = random.randint(30, 180)
                penalties = round(emi_amount * 0.05, 2)  # 5% penalty
                pending_principal = principal_amount
                pending_interest = interest_amount
                collection_agent_id = random.randint(1, 50) if random.random() < 0.7 else None

            # Ensure string fields are not None
            payment_mode = random.choice(payment_modes) if payment_status in ['Paid', 'Partial'] else ''
            bounce_reason = bounce_reason if bounce_reason is not None else ''

            repayments.append({
                'repayment_id': repayment_id,
                'loan_id': loan_id,
                'customer_id': customer_id,
                'emi_number': emi_number,
                'due_date': due_date,
                'payment_date': payment_date,
                'emi_amount': emi_amount,
                'principal_amount': principal_amount,
                'interest_amount': interest_amount,
                'penalties': penalties,
                'payment_status': payment_status,
                'payment_mode': payment_mode,
                'pending_principal': pending_principal,
                'pending_interest': pending_interest,
                'days_overdue': days_overdue,
                'bounce_reason': bounce_reason,
                'collection_agent_id': collection_agent_id
            })

            repayment_id += 1
            if payment_status in ['Paid', 'Partial']:
                outstanding_principal -= (principal_amount - pending_principal)

    # Insert repayments in batches
    batch_size = 10000
    for i in range(0, len(repayments), batch_size):
        batch = repayments[i:i + batch_size]
        ch_client.execute('INSERT INTO Fact_Loan_Repayment VALUES', batch)
        print(f'Inserted {len(batch)} repayment records (Total: {i + len(batch)}/{len(repayments)})')

def generate_dim_loan():
    print("Generating Dim_Loan data...")

    # Fetch all approved sales in one query
    approved_sales = ch_client.execute('''
        SELECT fs.sale_id, fs.customer_id, fs.product_id, fs.date_id, dc.risk_profile, fs.channel_id
        FROM Fact_Sales fs
        JOIN Dim_Customer dc ON fs.customer_id = dc.customer_id
        WHERE fs.status = 'Approved'
    ''')

    # Fetch all product categories in one query
    product_categories = ch_client.execute('SELECT product_id, category FROM Dim_Product')
    product_category_map = {row[0]: row[1] for row in product_categories}

    # Fetch all channel names in one query
    channel_names = ch_client.execute('SELECT channel_id, channel_name FROM Dim_Sales_Channel')
    channel_name_map = {row[0]: row[1] for row in channel_names}

    dim_loans = []
    total_sales = len(approved_sales)

    for idx, sale in enumerate(approved_sales):
        sale_id, customer_id, product_id, date_id, risk_profile, channel_id = sale

        # Get application date
        app_date = ch_client.execute(
            'SELECT date FROM Dim_Time WHERE date_id = %(date_id)s',
            {'date_id': date_id}
        )[0][0]

        # Determine interest rate based on risk
        if risk_profile == 'Low':
            interest_rate = round(random.uniform(3.0, 5.0), 1)
        elif risk_profile == 'Medium':
            interest_rate = round(random.uniform(5.1, 8.0), 1)
        else:
            interest_rate = round(random.uniform(8.1, 15.0), 1)

        # Loan details
        loan_amount = round(random.uniform(1000, 500000), 2)
        term = random.choice([12, 24, 36, 60, 84, 120, 180, 240, 360])
        start_date = app_date
        end_date = start_date + timedelta(days=term * 30)

        # Loan status with realistic distribution
        status = random.choices(
            ['Active', 'Closed', 'Defaulted', 'Delinquent'],
            weights=[0.7, 0.2, 0.05, 0.05]
        )[0]

        # Get product category for loan type
        product_category = product_category_map.get(product_id, 'Personal Loan')

        # Application channel
        channel_name = channel_name_map.get(channel_id, 'Unknown')

        # Calculate last_payment_date and next_payment_due_date
        last_payment_date = None
        next_payment_due_date = None

        if status in ['Active', 'Delinquent']:
            # Ensure end_date is in the future
            if end_date > datetime.now().date():
                last_payment_date = fake.date_between(start_date=start_date, end_date='today')
                next_payment_due_date = fake.date_between(start_date='today', end_date=end_date)
            else:
                # If end_date is in the past, set status to 'Closed'
                status = 'Closed'

        dim_loans.append({
            'loan_id': idx + 1,
            'customer_id': customer_id,
            'loan_amount': loan_amount,
            'interest_rate': interest_rate,
            'term_months': term,
            'start_date': start_date,
            'end_date': end_date,
            'loan_status': status,
            'loan_type': product_category,
            'risk_rating': risk_profile,
            'collateral_value': round(loan_amount * random.uniform(0.8, 1.5), 2) if product_category in ['Mortgage', 'Auto Loan'] else 0.0,
            'application_channel': channel_name,
            'application_date': start_date,
            'last_payment_date': last_payment_date,
            'next_payment_due_date': next_payment_due_date,
            'outstanding_balance': 0.0 if status == 'Closed' else round(loan_amount * random.uniform(0.1, 0.9), 2)
        })

        # Print progress every 1000 records
        if (idx + 1) % 1000 == 0:
            print(f"Processed {idx + 1}/{total_sales} sales...")

    # Insert loans in batches
    batch_size = 10000
    for i in range(0, len(dim_loans), batch_size):
        batch = dim_loans[i:i + batch_size]
        ch_client.execute('INSERT INTO Dim_Loan VALUES', batch)
        print(f"Inserted {len(batch)} loan records (Total: {i + len(batch)}/{len(dim_loans)})")

    print(f"Inserted {len(dim_loans)} loan records.")

def alter_regions():
    # Drop the table before inserting new data
    ch_client.execute('DROP TABLE IF EXISTS Dim_Region')
    print("Dropping tables")

    # Recreate the table (modify as per schema)
    ch_client.execute('''
        CREATE TABLE Dim_Region (
            region_id UInt32,
            region_name String,
            country String,
            latitude Float32,
            longitude Float32,
            sales_manager String
        ) ENGINE = MergeTree()
        ORDER BY region_id
    ''')

    # List of Southeast Asian countries, their regions, and approximate latitudes/longitudes
    # List of regions from the image
    regions_data = [
        {"region_name": "Bandar Seri Begawan", "country": "Brunei", "latitude": 4.9031, "longitude": 114.9398},
        {"region_name": "Phnom Penh", "country": "Cambodia", "latitude": 11.5564, "longitude": 104.9282},
        {"region_name": "Jakarta", "country": "Indonesia", "latitude": -6.2088, "longitude": 106.8456},
        {"region_name": "Vientiane", "country": "Laos", "latitude": 17.9757, "longitude": 102.6331},
        {"region_name": "Kuala Lumpur", "country": "Malaysia", "latitude": 3.139, "longitude": 101.6869},
        {"region_name": "Yangon", "country": "Myanmar", "latitude": 16.8661, "longitude": 96.1951},
        {"region_name": "Manila", "country": "Philippines", "latitude": 14.5995, "longitude": 120.9842},
        {"region_name": "Singapore", "country": "Singapore", "latitude": 1.3521, "longitude": 103.8198},
        {"region_name": "Bangkok", "country": "Thailand", "latitude": 13.7563, "longitude": 100.5018},
        {"region_name": "Hanoi", "country": "Vietnam", "latitude": 21.0285, "longitude": 105.8542}
    ]

    dim_regions = []
    for region_id, region in enumerate(regions_data, start=1):  # Start region_id from 1
        dim_regions.append({
            'region_id': region_id,
            'region_name': region['region_name'],
            'country': region['country'],
            'latitude': float(region['latitude']),  # Explicitly cast to float
            'longitude': float(region['longitude']),  # Explicitly cast to float
            'sales_manager': fake.name()
        })

    # Insert into ClickHouse
    print("Inserting into CH")
    ch_client.execute('INSERT INTO Dim_Region VALUES', dim_regions)


# Function to generate random account numbers
def generate_account_number():
    return str(uuid.uuid4())[:12]  # Shortened UUID

# Function to generate financial product values
def random_amount(min_val, max_val):
    return round(random.uniform(min_val, max_val), 2)

def random_duration(min_years, max_years):
    return random.randint(min_years, max_years)

def alter_customer():
    print("Adding additional Columns")
    ch_client.execute('''
        ALTER TABLE Dim_Customer
        ADD COLUMN IF NOT EXISTS has_fixed_deposit Bool DEFAULT 0,
        ADD COLUMN IF NOT EXISTS fd_account_number Nullable(String),
        ADD COLUMN IF NOT EXISTS fd_amount Nullable(Float64),
        ADD COLUMN IF NOT EXISTS fd_duration Nullable(Int32),
        ADD COLUMN IF NOT EXISTS has_savings_account Bool DEFAULT 0,
        ADD COLUMN IF NOT EXISTS savings_account_number Nullable(String),
        ADD COLUMN IF NOT EXISTS savings_balance Nullable(Float64),
        ADD COLUMN IF NOT EXISTS has_current_account Bool DEFAULT 0,
        ADD COLUMN IF NOT EXISTS current_account_number Nullable(String),
        ADD COLUMN IF NOT EXISTS current_balance Nullable(Float64),
        ADD COLUMN IF NOT EXISTS has_life_insurance Bool DEFAULT 0,
        ADD COLUMN IF NOT EXISTS life_insurance_policy_number Nullable(String),
        ADD COLUMN IF NOT EXISTS life_insurance_amount Nullable(Float64),
        ADD COLUMN IF NOT EXISTS life_insurance_term Nullable(Int32),
        ADD COLUMN IF NOT EXISTS has_general_insurance Bool DEFAULT 0,
        ADD COLUMN IF NOT EXISTS general_insurance_policy_number Nullable(String),
        ADD COLUMN IF NOT EXISTS general_insurance_coverage Nullable(Float64),
        ADD COLUMN IF NOT EXISTS general_insurance_term Nullable(Int32)
    ''')
    print("Assigning values dynamically using ALTER TABLE UPDATE")
    ch_client.execute('''
        ALTER TABLE Dim_Customer UPDATE
            has_fixed_deposit = IF(risk_profile IN ('Low', 'Medium') AND average_balance > 50000 AND age_group IN ('30-40', '40-50', '50+'), 1, has_fixed_deposit),
            fd_account_number = IF(has_fixed_deposit = 1, toString(generateUUIDv4()), fd_account_number),
            fd_amount = IF(has_fixed_deposit = 1, CAST((rand() % 200000 + 50000) AS Float64), fd_amount),
            fd_duration = IF(has_fixed_deposit = 1, rand() % 5 + 1, fd_duration),
            has_savings_account = IF(risk_profile IN ('Low', 'Medium', 'High') AND average_balance > 10000, 1, has_savings_account),
            savings_account_number = IF(has_savings_account = 1, toString(generateUUIDv4()), savings_account_number),
            savings_balance = IF(has_savings_account = 1, CAST((rand() % 50000 + 10000) AS Float64), savings_balance),
            has_current_account = IF(is_cash_intensive = 1 OR business_risk_class IN ('Medium', 'High'), 1, has_current_account),
            current_account_number = IF(has_current_account = 1, toString(generateUUIDv4()), current_account_number),
            current_balance = IF(has_current_account = 1, CAST((rand() % 100000 + 20000) AS Float64), current_balance),
            has_life_insurance = IF(risk_profile IN ('Low', 'Medium'), 1, has_life_insurance),
            life_insurance_policy_number = IF(has_life_insurance = 1, toString(generateUUIDv4()), life_insurance_policy_number),
            life_insurance_amount = IF(has_life_insurance = 1, CAST((rand() % 500000 + 100000) AS Float64), life_insurance_amount),
            life_insurance_term = IF(has_life_insurance = 1, rand() % 30 + 10, life_insurance_term),
            has_general_insurance = IF(business_risk_class IN ('Medium Risk', 'High Risk'), 1, has_general_insurance),
            general_insurance_policy_number = IF(has_general_insurance = 1, toString(generateUUIDv4()), general_insurance_policy_number),
            general_insurance_coverage = IF(has_general_insurance = 1, CAST((rand() % 100000 + 20000) AS Float64), general_insurance_coverage),
            general_insurance_term = IF(has_general_insurance = 1, rand() % 10 + 1, general_insurance_term)
        WHERE 1 = 1
    ''')
    print("Finished altering columns")

def update_region_ids():
    # Get the list of valid region IDs from the updated Dim_Region table
    valid_region_ids = [row[0] for row in ch_client.execute('SELECT region_id FROM Dim_Region')]

    # Update region_id in Dim_Customer
    ch_client.execute('''
        ALTER TABLE Dim_Customer UPDATE
        region_id = arrayElement([%s], (rand() %% %s) + 1)
        WHERE region_id NOT IN (%s)
    ''' % (','.join(map(str, valid_region_ids)), len(valid_region_ids), ','.join(map(str, valid_region_ids))))

    # Update region_id in Fact_Sales
    ch_client.execute('''
        ALTER TABLE Fact_Sales UPDATE
        region_id = arrayElement([%s], (rand() %% %s) + 1)
        WHERE region_id NOT IN (%s)
    ''' % (','.join(map(str, valid_region_ids)), len(valid_region_ids), ','.join(map(str, valid_region_ids))))

    print("Updated region IDs in Dim_Customer, Fact_Sales")

if __name__ == '__main__':
    print("Starting data generation...")
    # create_tables()
    # generate_dimension_data()
    # generate_fact_sales(num_records=20000)
    # generate_dim_loan()
    # generate_loan_repayments()
    # print("Data generation complete!")
    alter_regions()
    update_region_ids()
    # alter_customer()
