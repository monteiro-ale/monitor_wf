CREATE TABLE IF NOT EXISTS SENSORS (
        idtemp SERIAL PRIMARY KEY,
        powerfactor VARCHAR,
        hydraulicpressure VARCHAR,
        temperature VARCHAR,
        timestamp VARCHAR
    );