Структура проєкту

sales_analytics/    
├── dbt_project.yml          # Конфігурація проекту    
├── packages.yml             # Залежності dbt    
├── README.md               # Документація    
├── analyses/               # Аналітичні запити    
│   ├── monthly_revenue_growth.sql    
│   ├── agent_performance_ranking.sql    
│   └── agents_above_average_discount.sql    
├── models/                 # Моделі даних    
│   ├── schema.yml         # Опис моделей та тести    
│   ├── staging/    
│   │   └── raw_sales_data.sql    
│   └── marts/    
│       └── fct_sales.sql    
└── seeds/                  # Тестові дані    
    └── sales_data_seed.csv    

    Опис вітрини даних fct_sales
Таблиця fct_sales містить наступні поля:
Основна інформація

referenceid - унікальний ID продажі
product_name - назва проданого продукту
sales_agents_list - список агентів через кому
country - країна покупця
campaign_name - назва кампанії
source - джерело продажі (chat/call)

Фінансові показники

company_revenue_from_sale - загальний дохід від продажі
rebill_revenue - дохід тільки від rebills
number_of_rebills - кількість rebills
discount_amount - сума знижки
returned_amount - сума повернених коштів

Дати в різних часових зонах

return_date_kyiv/utc/new_york - дати повернення
order_date_kyiv/utc/new_york - дати продажі
days_between_return_and_purchase - різниця в днях

Аналітичні запити
1. Зростання доходу по місяцях
Файл: analyses/monthly_revenue_growth.sql
Розраховує відсоткове зростання доходу від місяця до місяця.
2. Рейтинг агентів
Файл: analyses/agent_performance_ranking.sql
Для кожного агента визначає:

Середній дохід
Кількість продажів
Середню знижку
Рангове місце по доходу

3. Агенти з високими знижками
Файл: analyses/agents_above_average_discount.sql
Визначає агентів, які надають знижки вище загального середнього рівня.
Тести даних
Проект включає комплексні тести для валідації даних:

Тести унікальності - перевірка унікальності ID продажів
Тести на NULL - перевірка обов'язкових полів
Тести значень - перевірка допустимих значень
Тести виразів - перевірка логічних правил (наприклад, суми >= 0)
Тести рівності - порівняння кількості рядків між таблицями

Особливості обробки даних

Обробка NULL значень: Всі NULL значення замінюються на 'N/A' для текстових полів та 0/false для числових/логічних.
Агрегація агентів: Якщо декілька агентів брали участь в одній продажі, вони об'єднуються через кому.
Часові зони: Всі дати конвертуються з часової зони Києва в UTC та Нью-Йорк.
Розрахунок доходу: Дохід розраховується як: total_amount + total_rebill_amount - returned_amount
