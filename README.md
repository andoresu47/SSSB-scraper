# SSSB-scraper
System designed to scrape apartment offering info from SSSB's website, analyze it, and compute/predict the probability of getting a given apartment. 

### Setting up database:
#### As `root`:
- `CREATE DATABASE sssb_data encoding='UTF8';`
- `CREATE USER sssbuser WITH PASSWORD 'password';`
- `GRANT ALL PRIVILEGES ON DATABASE "sssb_data" to sssbuser;`

#### As `sssbuser`:
- `\c sssb_data`
- `\i 'sssb_schema.sql'`
