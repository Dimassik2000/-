"""

CREATE TABLE product (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT,
    price       REAL,
    quantity    INTEGER,
    fromCity    TEXT,
    isAvailable TEXT,
    views       INTEGER
);
"""