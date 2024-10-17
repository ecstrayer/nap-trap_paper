PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS mpra_constant(
    constant_name TEXT,
    constant_value TEXT,
    PRIMARY KEY (constant_name)
);

CREATE TABLE IF NOT EXISTS reporter(
    reporter_id INTEGER,
    reporter_name TEXT,
    insert_sequence TEXT,
    tags TEXT,
    PRIMARY KEY (reporter_name)    
);

CREATE TABLE IF NOT EXISTS feature(
    feature_id INTEGER,
    reporter_id INTEGER,
    feature_value REAL,
    PRIMARY KEY (feature_id, reporter_id),
    FOREIGN KEY (reporter_id) REFERENCES reporter(reporter_id)
);

CREATE TABLE IF NOT EXISTS feature_attribute(
    feature_id INTEGER,
    feature_name TEXT,
    feature_type TEXT,
    PRIMARY KEY (feature_name),
    FOREIGN KEY (feature_id) REFERENCES feature(feature_id)
    );


CREATE TABLE IF NOT EXISTS sample_attribute(
    sample_id INTEGER,
    sample_name TEXT,
    organism TEXT,
    collection_time REAL,
    library TEXT,
    experiment_name TEXT,
    PRIMARY KEY (experiment_name, sample_name, sample_id)
);


CREATE TABLE IF NOT EXISTS replicate_attribute(
    replicate_id INTEGER,
    replicate_name TEXT,
    sample_id INTEGER,
    PRIMARY KEY (replicate_id, replicate_name, sample_id),
    FOREIGN KEY (sample_id) REFERENCES sample_attribute(sample_id)
);


CREATE TABLE IF NOT EXISTS run_attribute(
    run_id INTEGER,
    run_name TEXT,
    run_type TEXT,
    replicate_id INTEGER,
    PRIMARY KEY (run_id),
    FOREIGN KEY (replicate_id) REFERENCES replicate_attribute(replicate_id)
);



CREATE TABLE IF NOT EXISTS data_attribute(
    data_id INTEGER,
    replicate_id INTEGER,
    run_ids_included TEXT,
    data_type TEXT,
    data_info TEXT,
    PRIMARY KEY(data_type, replicate_id),
    FOREIGN KEY (replicate_id) REFERENCES replicate_attribute(replicate_id)
);



CREATE TABLE IF NOT EXISTS run_to_data_iso(
    run_id INTEGER,
    data_id INTEGER,
    PRIMARY KEY(run_id, data_id),
    FOREIGN KEY (run_id) REFERENCES run_attribute(run_id),
    FOREIGN KEY (data_id) REFERENCES data_attribute(data_id)
);


CREATE TABLE IF NOT EXISTS data_group_to_data_iso(
    data_group_id INTEGER,
    data_id INTEGER,
    PRIMARY KEY(data_group_id, data_id),
    FOREIGN KEY (data_group_id) REFERENCES data_group_attribute_iso(data_group_id),
    FOREIGN KEY (data_id) REFERENCES data_attribute(data_id)
);


CREATE TABLE IF NOT EXISTS data_group_attribute_iso(
    data_group_id INTEGER,
    data_group_type TEXT,
    sample_id INTEGER,
    data_group_name TEXT,
    PRIMARY KEY(data_group_type, sample_id),
    FOREIGN KEY (sample_id) REFERENCES sample_attribute(sample_id)
);



CREATE TABLE IF NOT EXISTS reporter_group_attribute(
    reporter_group_id INTEGER,
    reporter_group_type TEXT,
    reporter_group_hash TEXT,
    reporter_group_name TEXT,
    reporter_group_description TEXT,
    PRIMARY KEY (reporter_group_hash),
    FOREIGN KEY (reporter_group_id) REFERENCES reporter_group_to_data_group_iso(reporter_group_id),
    FOREIGN KEY (reporter_group_id) REFERENCES reporter_group_to_reporter_iso(reporter_group_id)
);


CREATE TABLE IF NOT EXISTS reporter_group_to_reporter_group_iso(
    reporter_group_id INTEGER,
    reporter_group_parent_id INTEGER,
    PRIMARY KEY (reporter_group_id, reporter_group_parent_id),
    FOREIGN KEY (reporter_group_id) REFERENCES reporter_group_attribute(reporter_group_id)
);


CREATE TABLE IF NOT EXISTS reporter_group_to_data_group_iso(
    reporter_group_id INTEGER,
    data_group_id INTEGER,
    PRIMARY KEY (data_group_id, reporter_group_id),
    FOREIGN KEY (data_group_id) REFERENCES data_group_attribute_iso(data_group_id)
    FOREIGN KEY (reporter_group_id) REFERENCES reporter_group_attribute(reporter_group_id)
);


CREATE TABLE IF NOT EXISTS reporter_group_to_reporter_iso(
    reporter_group_id INTEGER,
    reporter_id INTEGER,
    PRIMARY KEY (reporter_id, reporter_group_id),
    FOREIGN KEY (reporter_id) REFERENCES reporter(reporter_id)
);


CREATE TABLE IF NOT EXISTS raw_data_iso(
    run_id INTEGER,
    reporter_id INTEGER,
    raw_count INTEGER,
    rpm FLOAT,
    normalized_count FLOAT,
    PRIMARY KEY (run_id, reporter_id),
    FOREIGN KEY (reporter_id) REFERENCES reporter(reporter_id),
    FOREIGN KEY (run_id) REFERENCES run_attribute(run_id)  
);


CREATE TABLE IF NOT EXISTS processed_data_iso(
    data_id INTEGER,
    reporter_id INTEGER,
    processed_data_value REAL,
    PRIMARY KEY (data_id, reporter_id),
    FOREIGN KEY (reporter_id) REFERENCES reporter(reporter_id),
    FOREIGN KEY (data_id) REFERENCES data_attribute(data_id)
);


CREATE TABLE IF NOT EXISTS sample_data_iso(
    data_group_id INTEGER,
    reporter_id INTEGER,
    data_value REAL,
    PRIMARY KEY (data_group_id, reporter_id),
    FOREIGN KEY (reporter_id) REFERENCES reporter(reporter_id),
    FOREIGN KEY (data_group_id) REFERENCES data_group_attribute_iso(data_group_id)
);