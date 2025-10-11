CREATE SCHEMA IF NOT EXISTS landing_tables;

CREATE OR REPLACE TABLE landing_tables.towns (
    town_name VARCHAR NOT NULL,
    train_line VARCHAR,
    PRIMARY KEY (town_name)
);

INSERT INTO landing_tables.towns 
VALUES 
    ('Baildon', 'Wharfedale'), 
    ('Bingley', 'Airedale'),
    ('Crossflatts', 'Airedale'),
    ('Guiseley', 'Wharfedale'),
    ('Ilkley', 'Wharfedale'),
    ('Keighley', 'Airedale'),
    ('Shipley', 'Airedale'),
    ('Skipton', 'Airedale');

SELECT * FROM landing_tables.towns;
