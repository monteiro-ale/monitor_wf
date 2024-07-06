CREATE TABLE maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    turbine_id INT REFERENCES turbines(turbine_id),
    maintenance_date TIMESTAMP,
    maintenance_type VARCHAR(255),
    notes TEXT
);
