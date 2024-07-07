CREATE OR REPLACE PROCEDURE alerts_generate()
LANGUAGE plpgsql
AS $$
BEGIN
    DROP TABLE IF EXISTS temp_pre_alerts;
    CREATE TEMPORARY TABLE temp_pre_alerts (
        turbine_id INTEGER,
        powerfactor NUMERIC,
        hydraulicpressure NUMERIC,
        temperature NUMERIC,
        alert_date TIMESTAMP
    );

    INSERT INTO temp_pre_alerts (turbine_id, powerfactor, hydraulicpressure, temperature, alert_date)
    SELECT 
        turbine_id::INTEGER,
        powerfactor::NUMERIC,
        hydraulicpressure::NUMERIC,
        temperature::NUMERIC,
        timestamp::TIMESTAMP 
    FROM sensors
    WHERE CAST(temperature as NUMERIC) > 24
        OR CAST(hydraulicpressure as NUMERIC) < 72
        OR CAST(powerfactor as NUMERIC) < 0.8;

        -- Create temporary table temp_alert_type
    CREATE TEMPORARY TABLE temp_alert_type (
        turbine_id INTEGER,
        alert_date TIMESTAMP,
        alert_type VARCHAR(50)
    );

    -- Insert into temp_alert_type
    INSERT INTO temp_alert_type (turbine_id, alert_date, alert_type)
    SELECT 
        turbine_id,
        alert_date,
        CASE 
            WHEN CAST(temperature as NUMERIC) > 24 THEN 'High Temperature'
            WHEN CAST(hydraulicpressure as NUMERIC) < 72 THEN 'Low Hydraulic Pressure'
            WHEN CAST(powerfactor as NUMERIC) < 0.8 THEN 'Low Energy Efficiency'
        END as alert_type
    FROM temp_pre_alerts;

    -- Drop temp_pre_alerts since it's no longer needed
    DROP TABLE IF EXISTS temp_pre_alerts;

    -- Insert into alerts table
    INSERT INTO alerts (turbine_id, alert_date, alert_type, severity, resolved)
    SELECT 
        turbine_id,
        alert_date,
        alert_type,
        CASE 
            WHEN alert_type = 'High Temperature' THEN 'High'
            WHEN alert_type = 'Low Hydraulic Pressure' THEN 'Medium' 
            WHEN alert_type = 'Low Energy Efficiency' THEN 'Low'
        END AS severity,
        FALSE AS resolved
    FROM temp_alert_type;

    -- Drop temp_alert_type since it's no longer needed
    DROP TABLE IF EXISTS temp_alert_type;

    RAISE NOTICE 'Procedure executed successfully';
END;
$$;
