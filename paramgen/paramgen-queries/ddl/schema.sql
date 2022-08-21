-- drop temporal entity tables
DROP TABLE IF EXISTS Person_window;
DROP TABLE IF EXISTS Person_studyAt_University_window;
DROP TABLE IF EXISTS Person_workAt_Company_window;
DROP TABLE IF EXISTS knows_window;

-- schema of temporal entity tables
CREATE TABLE Person_window(personId bigint not null);
CREATE TABLE Person_studyAt_University_window(personId bigint not null, universityId bigint not null);
CREATE TABLE Person_workAt_Company_window(personId bigint not null, companyId bigint not null);
CREATE TABLE knows_window(person1Id bigint not null, person2Id bigint not null);
