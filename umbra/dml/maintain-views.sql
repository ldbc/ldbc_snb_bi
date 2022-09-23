-- maintain materialized views

-- Copy posts into comments
INSERT INTO Message
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
FROM Post;

DROP TABLE IF EXISTS Post;

-- Comments attaching to existing Message trees
INSERT INTO Message
    WITH RECURSIVE Message_CTE(MessageId, RootPostId, RootPostLanguage, ContainerForumId, ParentMessageId) AS (
        -- first half of the union: Comments attaching directly to the existing tree
        SELECT
            Comment.id AS MessageId,
            Message.RootPostId AS RootPostId,
            Message.RootPostLanguage AS RootPostLanguage,
            Message.ContainerForumId AS ContainerForumId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
        FROM Comment
        JOIN Message
          ON Message.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
        UNION ALL
        -- second half of the union: Comments attaching newly inserted Comments
        SELECT
            Comment.id AS MessageId,
            Message_CTE.RootPostId AS RootPostId,
            Message_CTE.RootPostLanguage AS RootPostLanguage,
            Message_CTE.ContainerForumId AS ContainerForumId,
            Comment.ParentCommentId AS ParentMessageId
        FROM Comment
        JOIN Message_CTE
          ON FORCEORDER(Comment.ParentCommentId = Message_CTE.MessageId)
    )
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
    FROM Message_CTE
    JOIN Comment
      ON FORCEORDER(Message_CTE.MessageId = Comment.id)
;

DROP TABLE IF EXISTS Comment;

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

DROP TABLE IF EXISTS Person_likes_Comment;
DROP TABLE IF EXISTS Person_likes_Post;

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
    UNION ALL
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
;

DROP TABLE IF EXISTS Comment_hasTag_Tag;
DROP TABLE IF EXISTS Post_hasTag_Tag;

----------------------------------------------------------------------------------------------------
------------------------------------------- CLEANUP ------------------------------------------------
----------------------------------------------------------------------------------------------------

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    content varchar(2000) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
) WITH (storage = paged);
CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    imageFile varchar(40),
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    language varchar(40),
    content varchar(2000),
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
) WITH (storage = paged);

CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
    --, PRIMARY KEY(CommentId, TagId)
) WITH (storage = paged);
CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
    --, PRIMARY KEY(PostId, TagId)
) WITH (storage = paged);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
    --, PRIMARY KEY(PersonId, CommentId)
) WITH (storage = paged);
CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
    --, PRIMARY KEY(PersonId, PostId)
) WITH (storage = paged);
