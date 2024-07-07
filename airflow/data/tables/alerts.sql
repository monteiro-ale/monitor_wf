CREATE TABLE alerts (
    alert_id SERIAL PRIMARY KEY,
    turbine_id INT REFERENCES turbines(turbine_id),
    alert_date TIMESTAMP,
    alert_type VARCHAR(255),
    severity VARCHAR(50),
    resolved BOOLEAN
);
