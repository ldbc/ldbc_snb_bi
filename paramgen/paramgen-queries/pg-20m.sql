-- materialize table
CREATE TABLE same_university_knows AS
    SELECT p1joined.person1id AS person1Id, p1joined.person2id AS person2Id
    FROM (
      SELECT k.person1Id AS person1Id, k.person2Id AS person2Id, p1.universityId AS universityId
      FROM knows_window k
      JOIN Person_studyAt_University_window p1
        ON p1.personId = k.person1Id
    ) p1joined
    JOIN Person_studyAt_University_window p2
      ON p2.personId = p1joined.person2Id
     AND p2.universityId = p1joined.universityId
