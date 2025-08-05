use homedata;
create SCHEMA if not exists staging; -- receiving the data
create SCHEMA if not exists raw_data; -- historical data (needs backing up)
create SCHEMA if not exists assets; -- clean data for later use (should be possible to recreate from raw or backup)
create SCHEMA if not exists analysis; -- saved analysis (should be possible to recreate from clean)