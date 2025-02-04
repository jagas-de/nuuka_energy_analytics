create schema nuuka_energy_test;
CREATE TABLE nuuka_energy_test.energy_usage (
    id SERIAL PRIMARY KEY,
    reportingGroup text,
    location TEXT,
    create_dttm TIMESTAMP,
    value DOUBLE PRECISION,
    unit TEXT,
	insert_dttm timestamp,
	update_dttm timestamp
);

create table nuuka_energy_test.audit_log(run_id text,run_date date, msg text);