

"""
CREATE TABLE building (
    id           INTEGER    PRIMARY KEY AUTOINCREMENT,
    name         TEXT (256),
    street       TEXT (256),
    begin        TEXT (256),
    system       TEXT (256),
    tours_count  INTEGER,
    min_rating   INTEGER,
    time_on_game INTEGER
);
"""

"""
CREATE TABLE comment (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id         REFERENCES building (id),
    place       INTEGER,
    prise       INTEGER
);
"""


PRAGMA foreign_keys = 0;

CREATE TABLE sqlitestudio_temp_table AS SELECT *
                                          FROM comment;

DROP TABLE comment;

CREATE TABLE comment (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id         REFERENCES building (id),
    place       INTEGER,
    prise_text  INTEGER
);

INSERT INTO comment (
                        id,
                        building_id,
                        place,
                        prise_text
                    )
                    SELECT id,
                           building_id,
                           place,
                           prise_text
                      FROM sqlitestudio_temp_table;

DROP TABLE sqlitestudio_temp_table;

PRAGMA foreign_keys = 1;