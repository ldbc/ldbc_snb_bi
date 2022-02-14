MATCH (personA:Person)-[knows:KNOWS]-(personB:Person)
OPTIONAL MATCH (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
WITH knows, CASE WHEN u IS NOT NULL THEN abs(saA.classYear - saB.classYear) + 1 ELSE 2^63-1 END AS weight
SET knows.q20weight = weight
