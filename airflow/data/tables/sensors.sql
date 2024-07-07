CREATE TABLE IF NOT EXISTS SENSORS (
        idtemp SERIAL PRIMARY KEY,
				turbine_id VARCHAR,
        powerfactor VARCHAR,
        hydraulicpressure VARCHAR,
        temperature VARCHAR,
        timestamp VARCHAR
    );