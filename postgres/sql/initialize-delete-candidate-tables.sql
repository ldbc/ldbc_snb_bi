CREATE TABLE IF NOT EXISTS Person_Delete_candidates                (deletionDate timestamp without time zone not null, id bigint);
CREATE TABLE IF NOT EXISTS Forum_Delete_candidates                 (deletionDate timestamp without time zone not null, id bigint);
CREATE TABLE IF NOT EXISTS Comment_Delete_candidates               (deletionDate timestamp without time zone not null, id bigint);
CREATE TABLE IF NOT EXISTS Post_Delete_candidates                  (deletionDate timestamp without time zone not null, id bigint);
CREATE TABLE IF NOT EXISTS Person_likes_Comment_Delete_candidates  (deletionDate timestamp without time zone not null, src bigint, trg bigint);
CREATE TABLE IF NOT EXISTS Person_likes_Post_Delete_candidates     (deletionDate timestamp without time zone not null, src bigint, trg bigint);
CREATE TABLE IF NOT EXISTS Forum_hasMember_Person_Delete_candidates(deletionDate timestamp without time zone not null, src bigint, trg bigint);
CREATE TABLE IF NOT EXISTS Person_knows_Person_Delete_candidates   (deletionDate timestamp without time zone not null, src bigint, trg bigint);
