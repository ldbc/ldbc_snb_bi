-- the 'contraints.sql' file serves multiple goals:
-- it inserts PK constraints, FK indexes, views, and bidirectional edges

-- bidirectional Person_knows_Person edges
INSERT INTO Person_knows_Person (creationDate, Person1Id, Person2Id)
SELECT creationDate, Person2Id, Person1Id
FROM Person_knows_Person;

-- PKs

-- static nodes
ALTER TABLE Organisation ADD PRIMARY KEY (id);
ALTER TABLE Place ADD PRIMARY KEY (id);
ALTER TABLE Tag ADD PRIMARY KEY (id);
ALTER TABLE TagClass ADD PRIMARY KEY (id);

-- dynamic nodes
ALTER TABLE Comment ADD PRIMARY KEY (id);
ALTER TABLE Forum ADD PRIMARY KEY (id);
ALTER TABLE Post ADD PRIMARY KEY (id);
ALTER TABLE Person ADD PRIMARY KEY (id);

-- dynamic edges
ALTER TABLE Comment_hasTag_Tag ADD PRIMARY KEY (CommentId, TagId);
ALTER TABLE Post_hasTag_Tag ADD PRIMARY KEY (PostId, TagId);
ALTER TABLE Forum_hasMember_Person ADD PRIMARY KEY (ForumId, PersonId);
ALTER TABLE Forum_hasTag_Tag ADD PRIMARY KEY (ForumId, TagId);
ALTER TABLE Person_hasInterest_Tag ADD PRIMARY KEY (PersonId, TagId);
ALTER TABLE Person_likes_Comment ADD PRIMARY KEY (PersonId, CommentId);
ALTER TABLE Person_likes_Post ADD PRIMARY KEY (PersonId, PostId);
ALTER TABLE Person_studyAt_University ADD PRIMARY KEY (PersonId, UniversityId);
ALTER TABLE Person_workAt_Company ADD PRIMARY KEY (PersonId, CompanyId);
ALTER TABLE Person_knows_Person ADD PRIMARY KEY (Person1Id, Person2Id);

-- Views

CREATE VIEW Message AS
    SELECT creationDate, id, content, NULL AS imageFile, locationIP, browserUsed, NULL AS language, length, CreatorPersonId, NULL AS ContainerForumId, LocationCountryId, coalesce(ParentPostId, ParentCommentId) AS ParentMessageId
    FROM Comment
    UNION ALL
    SELECT creationDate, id, content, imageFile, locationIP, browserUsed, language, length, CreatorPersonId, ContainerForumId, LocationCountryId, NULL AS ParentMessageId
    FROM Post
;

-- recursive view containing the root Post of each Message (for Posts, themselves; for Comments, traversing up the Message thread to the root Post of the tree)
CREATE VIEW MessageThread AS
    WITH RECURSIVE MessageThread_CTE(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile, locationIP, browserUsed, language, length, CreatorPersonId, ContainerForumId, LocationCountryId, ParentMessageId, type) AS (
        SELECT
            creationDate,
            id AS MessageId,
            id AS RootPostId,
            language AS RootPostLanguage,
            content,
            imageFile,
            locationIP,
            browserUsed,
            language,
            length,
            CreatorPersonId,
            ContainerForumId,
            LocationCountryId,
            NULL::bigint AS ParentMessageId,
            'Post' AS type
        FROM Post
        UNION ALL
        SELECT
            Comment.creationDate AS creationDate,
            Comment.id AS MessageId,
            MessageThread_CTE.RootPostId AS RootPostId,
            MessageThread_CTE.RootPostLanguage AS RootPostLanguage,
            Comment.content AS content,
            NULL::varchar(40) AS imageFile,
            Comment.locationIP AS locationIP,
            Comment.browserUsed AS browserUsed,
            NULL::varchar(40) AS language,
            Comment.length AS length,
            Comment.CreatorPersonId AS CreatorPersonId,
            MessageThread_CTE.ContainerForumId AS ContainerForumId,
            Comment.LocationCountryId AS LocationCityId,
            coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId,
            'Comment' AS type
        FROM Comment, MessageThread_CTE
        WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = MessageThread_CTE.MessageId
    )
    SELECT * FROM MessageThread_CTE;

CREATE VIEW Person_likes_Message AS
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
;

CREATE VIEW Message_hasTag_Tag AS
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
    UNION ALL
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
;

CREATE VIEW Country AS
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place
    WHERE type = 'Country'
;

CREATE VIEW City AS
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place
    WHERE type = 'City'
;

-- Indexes for FKs

-- merged FKs / static nodes
CREATE INDEX Organisation_LocationPlaceId ON Organisation (LocationPlaceId);
CREATE INDEX Place_PartOfPlaceId ON Place (PartOfPlaceId);
CREATE INDEX Tag_TypeTagClassId ON Tag (TypeTagClassId);
CREATE INDEX TagClass_SubclassOfTagClassId ON TagClass (SubclassOfTagClassId);

-- merged FKs / dynamic nodes
CREATE INDEX Forum_ModeratorPersonId ON Forum (ModeratorPersonId);
CREATE INDEX Person_LocationCityId ON Person (LocationCityId);
CREATE INDEX Comment_CreatorPersonId ON Comment (CreatorPersonId);
CREATE INDEX Comment_LocationCountryId ON Comment (LocationCountryId);
CREATE INDEX Comment_ParentPostId ON Comment (ParentPostId);
CREATE INDEX Comment_ParentCommentId ON Comment (ParentCommentId);
CREATE INDEX Post_CreatorPersonId ON Post (CreatorPersonId);
CREATE INDEX Post_ContainerForumId ON Post (ContainerForumId);
CREATE INDEX Post_LocationCountryId ON Post (LocationCountryId);

-- edge sources
CREATE INDEX Comment_hasTag_Tag_CommentId ON Comment_hasTag_Tag(CommentId);
CREATE INDEX Post_hasTag_Tag_PostId ON Post_hasTag_Tag(PostId);
CREATE INDEX Forum_hasMember_Person_ForumId ON Forum_hasMember_Person(ForumId);
CREATE INDEX Forum_hasTag_Tag_ForumId ON Forum_hasTag_Tag(ForumId);
CREATE INDEX Person_hasInterest_Tag_PersonId ON Person_hasInterest_Tag(PersonId);
CREATE INDEX Person_likes_Comment_PersonId ON Person_likes_Comment(PersonId);
CREATE INDEX Person_likes_Post_PersonId ON Person_likes_Post(PersonId);
CREATE INDEX Person_studyAt_University_PersonId ON Person_studyAt_University(PersonId);
CREATE INDEX Person_workAt_Company_PersonId ON Person_workAt_Company(PersonId);
CREATE INDEX Person_knows_Person_Person1id ON Person_knows_Person(Person1id);

-- edge targets
CREATE INDEX Comment_hasTag_Tag_TagId ON Comment_hasTag_Tag(TagId);
CREATE INDEX Post_hasTag_Tag_TagId ON Post_hasTag_Tag(TagId);
CREATE INDEX Forum_hasMember_Person_PersonId ON Forum_hasMember_Person(PersonId);
CREATE INDEX Forum_hasTag_Tag_TagId ON Forum_hasTag_Tag(TagId);
CREATE INDEX Person_hasInterest_Tag_TagId ON Person_hasInterest_Tag(TagId);
CREATE INDEX Person_likes_Comment_CommentId ON Person_likes_Comment(CommentId);
CREATE INDEX Person_likes_Post_PostId ON Person_likes_Post(PostId);
CREATE INDEX Person_studyAt_University_UniversityId ON Person_studyAt_University(UniversityId);
CREATE INDEX Person_workAt_Company_CompanyId ON Person_workAt_Company(CompanyId);
CREATE INDEX Person_knows_Person_Person2id ON Person_knows_Person(Person2id);
