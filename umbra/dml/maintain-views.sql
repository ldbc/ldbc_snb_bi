-- maintain materialized views

-- Comments attaching to existing Message trees
INSERT INTO Message
    WITH RECURSIVE Message_CTE(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, locationIP, browserUsed, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId, type) AS (
        -- first half of the union: Comments attaching directly to the existing tree
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message.RootPostId AS RootPostId,
            Message.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            Comment.ParentPostId,
            Comment.ParentCommentId,
            'Comment' AS type
        FROM Comment
        JOIN Message
          ON Message.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
        UNION ALL
        -- second half of the union: Comments attaching newly inserted Comments
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            Comment.ParentPostId,
            Comment.ParentCommentId,
            'Comment' AS type
        FROM Comment
        JOIN Message_CTE
          ON Comment.ParentCommentId = Message_CTE.MessageId
    )
    SELECT * FROM Message_CTE
;

-- Posts and Comments to new Message trees
INSERT INTO Message
    WITH RECURSIVE Message_CTE(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, locationIP, browserUsed, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId, type) AS (
        SELECT
            creationDate,
            id AS MessageId,
            id AS RootPostId,
            language AS RootPostLanguage,
            content,
            imageFile,
            locationIP,
            browserUsed,
            length,
            CreatorPersonId,
            ContainerForumId,
            LocationCountryId,
            NULL::bigint AS ParentMessageId,
            NULL::bigint AS ParentPostId,
            NULL::bigint AS ParentCommentId,
            'Post' AS type
        FROM Post
        UNION ALL
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            Comment.ParentPostId,
            Comment.ParentCommentId,
            'Comment' AS type
        FROM Comment, Message_CTE
        WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = Message_CTE.MessageId
    )
    SELECT * FROM Message_CTE
;

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
    UNION ALL
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
;

DROP TABLE IF EXISTS PathQ19;
CREATE TABLE PathQ19 (
    src bigint,
    dst bigint,
    w double precision,
    primary key (src, dst)
) with (storage = paged);
INSERT INTO PathQ19(src, dst, w)
WITH
weights(src, dst, c) as (
    select least(m1.creatorpersonid, m2.creatorpersonid) as src,
           greatest(m1.creatorpersonid, m2.creatorpersonid) as dst,
           count(*) as c
    from Person_knows_person pp, Message m1, Message m2
    where pp.person1id = m1.creatorpersonid and pp.person2id = m2.creatorpersonid and m1.parentmessageid = m2.messageid and m1.creatorpersonid <> m2.creatorpersonid
    group by src, dst
)
select src, dst, 1.0::double precision / c from weights
union all
select dst, src, 1.0::double precision / c from weights;


DROP TABLE IF EXISTS PathQ20;
CREATE TABLE PathQ20 (
    src bigint,
    dst bigint,
    w int,
    primary key (src, dst)
) with (storage = paged);
INSERT INTO PathQ20(src, dst, w)
select p1.personid, p2.personid, min(abs(p1.classYear - p2.classYear)) + 1
from Person_knows_person pp, Person_studyAt_University p1, Person_studyAt_University p2
where pp.person1id = p1.personid and pp.person2id = p2.personid and p1.universityid = p2.universityid
group by p1.personid, p2.personid;


DELETE FROM Comment;
DELETE FROM Post;

DELETE FROM Comment_hasTag_Tag;
DELETE FROM Post_hasTag_Tag;

DELETE FROM Person_likes_Comment;
DELETE FROM Person_likes_Post;
