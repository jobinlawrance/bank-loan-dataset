erDiagram
    Dim_Time {
        Int32 date_id
        Date date
        Int8 month
        Int16 year
    }

    Dim_Product {
        Int32 product_id
        String product_name
        String category
        Float64 price
        Int32 supplier_id
    }

    Dim_Region {
        Int32 region_id
        String region_name
        String country
        String sales_manager
    }

    Dim_Sales_Channel {
        Int32 channel_id
        String channel_name
        String platform
    }

    Dim_Customer {
        Int32 customer_id
        String name
        Int32 region_id
        String age_group
        String gender
        String membership_status
        Float64 average_balance
        Float64 average_income
        String business_risk_class
        Bool is_pep
        Float64 account_balance
        Bool is_cash_intensive
        Bool tpr_threshold_exceeded
        Bool transacts_hr_jurisdictions
        String preferred_channel
        String[] interests
        String occupation
        String lifecycle_stage
        Float64 churn_risk_score
        Float64 predicted_clv
        Bool consent_marketing
        Bool consent_data_share
        Date data_deletion_date
    }

    Fact_Sales {
        Int32 sale_id
        Int32 date_id
        Int32 product_id
        Int32 customer_id
        Int32 region_id
        Int32 channel_id
        Int32 units_sold
        Float64 revenue
        Float64 discount_amount
    }

    Dim_Loan {
        Int32 loan_id
        Int32 customer_id
        Float64 loan_amount
        Float32 interest_rate
        Int16 term_months
        Date start_date
        Date end_date
        String loan_status
        String loan_type
        String risk_rating
        Float64 collateral_value
        String application_channel
        Date application_date
        Date last_payment_date
        Date next_payment_due_date
        Float64 outstanding_balance
    }

    Fact_Sales ||--o{ Dim_Time: "date_id"
    Fact_Sales ||--o{ Dim_Product: "product_id"
    Fact_Sales ||--o{ Dim_Customer: "customer_id"
    Fact_Sales ||--o{ Dim_Region: "region_id"
    Fact_Sales ||--o{ Dim_Sales_Channel: "channel_id"
    Dim_Customer ||--o{ Dim_Region: "region_id"
    Dim_Loan ||--o{ Dim_Customer: "customer_id"
