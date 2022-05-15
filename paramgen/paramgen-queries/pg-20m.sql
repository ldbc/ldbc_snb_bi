-- materialize table
CREATE TABLE same_university_knows_window AS
    SELECT sameUniversityKnows.person1Id AS person1Id, sameUniversityKnows.person2Id AS person2Id
    FROM sameUniversityKnows
    -- the 'knows' edge exists within the time window
    JOIN knows_window
      ON knows_window.person1Id = sameUniversityKnows.person1Id
     AND knows_window.person2Id = sameUniversityKnows.person2Id
    -- the 'same university' constraint holds within the time window
    JOIN Person_studyAt_University_window psuw1
      ON psuw1.personId = sameUniversityKnows.person1Id
    JOIN Person_studyAt_University_window psuw2
      ON psuw2.personId = sameUniversityKnows.person2Id
     AND psuw2.universityId = psuw1.universityId
;
