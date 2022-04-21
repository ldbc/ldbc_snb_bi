-- materialize table
CREATE TABLE same_university_knows AS
    SELECT k.person1id AS person1Id, k.person2id AS person2Id
    FROM knows_window k
    JOIN Person_studyAt_Univesity_window p1
      ON p1.personId = k.person1Id
    JOIN Person_studyAt_Univesity_window p2
      ON p2.personId = k.person2Id
     AND p2.universityId = p1.universityId
