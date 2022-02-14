MATCH (personA:Person)-[knows:KNOWS]-(personB:Person)
MATCH (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
SET knows.q20weight = abs(saA.classYear - saB.classYear) + 1
