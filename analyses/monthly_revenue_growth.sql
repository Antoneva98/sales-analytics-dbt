-- Розрахунок відсоткового зростання доходу від місяця до місяця
with monthly_revenue as (
    select 
        date_trunc('month', order_date_kyiv)::date as month_year,
        sum(company_revenue_from_sale) as total_revenue
    from {{ ref('fct_sales') }}
    where order_date_kyiv >= '2020-01-01' -- Виключаємо default дати
    group by date_trunc('month', order_date_kyiv)::date
    order by month_year
),

revenue_with_lag as (
    select 
        month_year,
        total_revenue,
        lag(total_revenue) over (order by month_year) as previous_month_revenue
    from monthly_revenue
)

select 
    month_year,
    total_revenue,
    previous_month_revenue,
    case 
        when previous_month_revenue is null or previous_month_revenue = 0 then null
        else round(
            ((total_revenue - previous_month_revenue) / previous_month_revenue) * 100, 2
        )
    end as revenue_growth_percentage
from revenue_with_lag
order by month_year;
