SQL migration files for https://github.com/golang-migrate/migrate

Migrations should be
(1) as backwards compatible as possible for older clients
(2) defer destroying unwanted data
(3) use nondestructive "down" steps where possible