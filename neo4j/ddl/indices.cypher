// uniqueness constrains (implying an index)
CREATE CONSTRAINT FOR (n:City)         REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Comment)      REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Country)      REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Forum)        REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Message)      REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Organisation) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Person)       REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Post)         REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT FOR (n:Tag)          REQUIRE n.id IS UNIQUE;

// name/firstName
CREATE INDEX FOR (n:Country)   ON n.name;
CREATE INDEX FOR (n:Person)    ON n.firstName;
CREATE INDEX FOR (n:Tag)       ON n.name;
CREATE INDEX FOR (n:TagClass)  ON n.name;

// creationDate of nodes
CREATE INDEX FOR (n:Message)   ON n.creationDate;
CREATE INDEX FOR (n:Post)      ON n.creationDate;
CREATE INDEX FOR (n:Forum)     ON n.creationDate;

// creationDate of edges
CREATE INDEX FOR ()-[e:KNOWS]-() ON e.creationDate;
