#### Лабораторную работу № 3 выполнил Лернер Феликс Леонидович, группа М8О-310Б-23.

Первый запуск и заполнение таблиц: 

`docker compose --profile run-etl up --build`

Запуск без повторного заполнения таблиц:

`docker compose up -d`

Запуск командной строки postgres: 

`docker exec -it postgres_star psql -U flink_user -d star_schema`.
