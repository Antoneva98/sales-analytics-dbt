{{ config(materialized='table') }}

with sales_data as (
    select 
        referenceid,
        coalesce(country, 'N/A') as country,
        coalesce(product_code, 'N/A') as product_code,
        coalesce(product_name, 'N/A') as product_name,
        coalesce(subscription_start_date, '1900-01-01') as subscription_start_date,
        coalesce(subscription_deactivation_date, '1900-01-01') as subscription_deactivation_date,
        coalesce(subscription_duration_months, 0) as subscription_duration_months,
        coalesce(order_date_kyiv, '1900-01-01 00:00:00') as order_date_kyiv,
        coalesce(return_date_kyiv, '1900-01-01 00:00:00') as return_date_kyiv,
        coalesce(last_revill_date_kyiv, '1900-01-01 00:00:00') as last_revill_date_kyiv,
        coalesce(has_chargeback, false) as has_chargeback,
        coalesce(has_refund, false) as has_refund,
        coalesce(sales_agent_name, 'N/A') as sales_agent_name,
        coalesce(source, 'N/A') as source,
        coalesce(campaign_name, 'N/A') as campaign_name,
        coalesce(total_amount, 0) as total_amount,
        coalesce(discount_amount, 0) as discount_amount,
        coalesce(number_of_rebills, 0) as number_of_rebills,
        coalesce(original_amount, 0) as original_amount,
        coalesce(returned_amount, 0) as returned_amount,
        coalesce(total_rebill_amount, 0) as total_rebill_amount
    from {{ ref('raw_sales_data') }}
),

sales_with_agents as (
    select 
        referenceid,
        country,
        product_code,
        product_name,
        subscription_start_date,
        subscription_deactivation_date,
        subscription_duration_months,
        order_date_kyiv,
        return_date_kyiv,
        last_revill_date_kyiv,
        has_chargeback,
        has_refund,
        -- Aggregate agents for each sale (in case there are multiple records per sale)
        string_agg(distinct sales_agent_name, ', ') as sales_agents_list,
        source,
        campaign_name,
        max(total_amount) as total_amount,
        max(discount_amount) as discount_amount,
        max(number_of_rebills) as number_of_rebills,
        max(original_amount) as original_amount,
        max(returned_amount) as returned_amount,
        max(total_rebill_amount) as total_rebill_amount
    from sales_data
    group by 
        referenceid, country, product_code, product_name, subscription_start_date,
        subscription_deactivation_date, subscription_duration_months, order_date_kyiv,
        return_date_kyiv, last_revill_date_kyiv, has_chargeback, has_refund,
        source, campaign_name
),

final_sales as (
    select
        referenceid,
        product_name,
        sales_agents_list,
        country,
        campaign_name,
        source,
        
        -- Дохід компанії від продажі (враховуючи rebills та повернення)
        total_amount + total_rebill_amount - returned_amount as company_revenue_from_sale,
        
        -- Дохід компанії тільки від rebills
        total_rebill_amount as rebill_revenue,
        
        -- Кількість rebills
        number_of_rebills,
        
        -- Сума знижки
        discount_amount,
        
        -- Сума повернених коштів
        returned_amount,
        
        -- Дати повернення коштів в різних часових зонах
        case 
            when return_date_kyiv = '1900-01-01 00:00:00' then null
            else return_date_kyiv
        end as return_date_kyiv,
        
        case 
            when return_date_kyiv = '1900-01-01 00:00:00' then null
            else return_date_kyiv at time zone 'Europe/Kiev' at time zone 'UTC'
        end as return_date_utc,
        
        case 
            when return_date_kyiv = '1900-01-01 00:00:00' then null
            else return_date_kyiv at time zone 'Europe/Kiev' at time zone 'America/New_York'
        end as return_date_new_york,
        
        -- Різниця днів між датою повернення та датою покупки
        case 
            when return_date_kyiv = '1900-01-01 00:00:00' then null
            else date_part('day', return_date_kyiv::date - order_date_kyiv::date)
        end as days_between_return_and_purchase,
        
        -- Дати продажі в різних часових зонах
        order_date_kyiv as order_date_kyiv,
        order_date_kyiv at time zone 'Europe/Kiev' at time zone 'UTC' as order_date_utc,
        order_date_kyiv at time zone 'Europe/Kiev' at time zone 'America/New_York' as order_date_new_york
        
    from sales_with_agents
)

select * from final_sales
