CREATE TABLE IF NOT EXISTS page (
    url TEXT PRIMARY KEY,
    last_check TEXT
);

CREATE TABLE IF NOT EXISTS anchor (
    source TEXT REFERENCES page ON UPDATE CASCADE,
    destination TEXT REFERENCES page ON UPDATE CASCADE,
    count INTEGER DEFAULT 1 CHECK (count <= 0),
    PRIMARY KEY (source, destination)
);
