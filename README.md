Название проекта
===========

Cоздание ETL-процесса формирования витрин данных для анализа изменений курса акций.

Описание задачи
===========
<details>
<summary> :white_check_mark: </summary>

Разработать скрипты загрузки данных в 2-х режимах:

• Инициализирующий — загрузка полного слепка данных источника

• Инкрементальный — загрузка дельты данных за прошедшие сутки

Организовать правильную структуру хранения данных:

• Сырой слой данных

• Промежуточный слой

• Слой витрин

В качестве результата работы программного продукта необходимо написать скрипт, который формирует витрину данных следующего содержания:

• Суррогатный ключ категории

• Название валюты

• Суммарный объем торгов за последние сутки

• Курс валюты на момент открытия торгов для данных суток

• Курс валюты на момент закрытия торгов для данных суток

• Разница (в %) курса с момента открытия до момента закрытия торгов для данных суток

• Минимальный временной интервал, на котором был зафиксирован самый крупный объем торгов для данных суток

• Минимальный временной интервал, на котором был зафиксирован максимальный курс для данных суток

• Минимальный временной интервал, на котором был зафиксирован минимальный курс торгов для данных суток

Дополнение:

В качестве основы витрины необходимо выбрать 3–5 различных валют или акций компаний.

Источники:

[Free Stock APIs in JSON & Excel | Alpha Vantage](https://www.alphavantage.co/) 

</details>


План реализации
===========
<details>
<summary> :white_check_mark: </summary>

1. Анализ источника данных для определения способа сбора информации

2. Проектирование структуры DWH

3. Определение стека используемых при реализации проекта  технологий 

4. Физическая реализация проекта: написание скриптов для создания БД, настройка соединений

</details>

Анализ источника данных для определения способа сбора информации
===========

<details>
<summary> :white_check_mark: </summary>

Заданный в качестве источника данных  API предоставляет глобальные данные по акциям в 4 различных временных разрешениях: (1) ежедневно, (2) еженедельно, (3) ежемесячно и (4) внутридневно.

Т.к. нам необходимо определить временные интервалы на которых был зафиксирован минимальный/максимальный курс торгов за сутки и самый крупный объем торгов за сутки - мы выбираем API возвращающий внутридневные временные ряды TIME_SERIES_INTRADAY.

API позволяет задать интервал времени между двумя последовательными точками данных во временном ряду. Поддерживаются следующие значения: 1min, 5min, 15min, 30min,60min.

Т.к. нам нужно найти минимальные временные интервалы, выбираем интервал равный 1min.

В качестве основы витрины выбираем 5 акций популярных компаний:

• AAPL – Apple

• NVDA – NVIDIA

• TSLA – Tesla

• BABA - Alibaba ADR

• META - Meta Platforms

</details>


ER-диаграмма
===========

<details>
<summary> :white_check_mark: </summary>

![Image alt](https://github.com/MOMIV/MProject/blob/main/doc/pic/ERD.png)

Используемые технологии с обоснованием
===========
<details>
<summary> :white_check_mark: </summary>

• **PostgreSQL**

Объем извлекаемымх данных не велик. 
С учетом часов работы рынков с 04.00 до 20.00 и интервала извлечения 1min мы получаем порядка 960 записей в день по каждой акции.
Выгружать данные можно за месяц, либо последние 100 точек данных. 
Количество записей за месяц 28800.
По 5 акциям 28800*5 = 144000 записей.

Извлекаемые данные структурированы, для их хранения и обработки подходят реляционные базы данных, такие как PostgreSQL.
 
• **Airflow**

Оркестратор, используемый для разработки, планирования и мониторинга рабочих процессов - имеет удобный веб-интерфейс для создания конвейеров данных, где можно наглядно отслеживать жизненный цикл данных в цепочках связанных задач.

</details>

Запуск проекта
===========

<details>
<summary> :white_check_mark: </summary>

1. Для запуска проекта необходимо склонировать проект на свою машину.
В терминале перейти в папку проекта и выполнить команду *docker-compose up -d*

2. Далее необходимо перейти в Airflow  по адресу [http://localhost:8080]( http://localhost:8080/)

3. Для загрузки Variables и Connections запускается DAG [01_conn_var]( https://github.com/MOMIV/MProject/blob/main/airflow/dags/01_conn_var.py)

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/Graph_01_conn_var.png)

Результат работы DAG 01_conn_var

![Image alt](https://github.com/MOMIV/MProject/blob/main/doc/pic/List_connection.png)

![Image alt](https://github.com/MOMIV/MProject/blob/main/doc/pic/List_variable.png)

Список DAG

![Image alt](https://github.com/MOMIV/MProject/blob/main/doc/pic/Dags.png)


4. К БД подключаемся с помощью DBeaver, параметры подключения берем из [connections.json](https://github.com/MOMIV/MProject/blob/main/airflow/dags/connections.json)

5. Далее запускается DAG [02_init](https://github.com/MOMIV/MProject/blob/main/airflow/dags/02_init.py) – инициализирующий

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/Graph_02_init.png)

Результат работы DAG 02_init

Данные по акциям Apple на row-слое

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_01_row.png)

Данные по акциям Apple на core-слое

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_01_core.png)

Витрина данных

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_01_mart.png)

6. DAG 02_init запускается один раз, далее данные дополняются ежедневно в 01.00 при запуске DAG [03_incr]( https://github.com/MOMIV/MProject/blob/main/airflow/dags/03_incr.py) – инкрементальный


![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/Graph_03_incr.png)

Результат работы DAG 03_incr

Данные по акциям Apple на row-слое

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_02_row.png)

Данные по акциям Apple на core-слое

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_02_core.png)

Витрина данных

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_02_mart.png)

</details>


Результаты разработки
===========

![Image alt]( https://github.com/MOMIV/MProject/blob/main/doc/pic/DB_after_DAG_02_mart.png)


Выводы
===========

1. Создан ETL-процесс формирования витрин данных для анализа изменений курса акций

2. В процессе выполнения работы закреплены на практике знания и навыки работы с изученными на курсе технологиями  используемыми в инженерии данных
