CREATE TABLE turbines (
    turbine_id SERIAL PRIMARY KEY,
    location VARCHAR(255),
    manufacturer VARCHAR(255),
    model VARCHAR(255),
    capacity DECIMAL(5,2)
);


INSERT INTO turbines (location, manufacturer, model, capacity) VALUES
('37.7749° N, 122.4194° W', 'Siemens Gamesa', 'SG 3.4-132', 3.4),
('51.5074° N, 0.1278° W', 'GE Renewable Energy', 'GE 1.7-100', 1.7),
('48.8566° N, 2.3522° E', 'Vestas', 'V90-3.0 MW', 3.0),
('40.7128° N, 74.0060° W', 'Nordex', 'N60/1300', 1.3),
('34.0522° N, 118.2437° W', 'Siemens Gamesa', 'SG 8.0-167 DD', 8.0),
('52.5200° N, 13.4050° E', 'GE Renewable Energy', 'GE 2.5-120', 2.5),
('35.6895° N, 139.6917° E', 'Vestas', 'V164-9.5 MW', 9.5),
('55.7558° N, 37.6173° E', 'Nordex', 'N117/2400', 2.4),
('45.4215° N, 75.6972° W', 'Siemens Gamesa', 'SG 5.0-145', 5.0),
('35.6762° N, 139.6503° E', 'GE Renewable Energy', 'GE 3.6-137', 3.6);