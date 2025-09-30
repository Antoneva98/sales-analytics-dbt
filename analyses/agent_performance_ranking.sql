-- Аналіз продуктивності агентів з ранжуванням
with agent_sales as (
    -- Розбиваємо список агентів на окремі рядки
    select 
        referenceid,
        trim(agent.value) as individual_agent,
        company_revenue_from_sale,
        discount_amount
    from {{ ref('fct_sales') }},
    lateral split_to_table(sales_agents_list, ',') as agent
    where trim(agent.value) != 'N/A' 
    and sales_agents_list != 'N/A'
),

agent_metrics as (
    select 
        individual_agent as agent_name,
        count(*) as total_sales_count,
        round(avg(company_revenue_from_sale), 2) as average_revenue_per_sale,
        round(sum(company_revenue_from_sale), 2) as total_revenue,
        round(avg(discount_amount), 2) as average_discount_per_sale
    from agent_sales
    group by individual_agent
),

ranked_agents as (
    select 
        agent_name,
        total_sales_count,
        average_revenue_per_sale,
        total_revenue,
        average_discount_per_sale,
        row_number() over (order by total_revenue desc) as revenue_rank
    from agent_metrics
)

select 
    revenue_rank,
    agent_name,
    total_sales_count,
    average_revenue_per_sale,
    total_revenue,
    average_discount_per_sale
from ranked_agents
order by revenue_rank;
