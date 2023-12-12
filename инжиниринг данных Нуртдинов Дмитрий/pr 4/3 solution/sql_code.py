"""
CREATE TABLE music (
    id          INTEGER    PRIMARY KEY AUTOINCREMENT,
    artist      TEXT (256),
    song        TEXT (256),
    duration_ms NUMERIC,
    year        INTEGER,
    tempo       REAL,
    genre       TEXT,
    popularity  INTEGER
);
"""