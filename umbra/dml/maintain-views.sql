-- maintain materialized views

-- Comments attaching to existing Message trees
UPDATE Message res
SET RootPostId = o.RootPostId, RootPostLanguage = o.RootPostLanguage, ContainerForumId = o.ContainerForumId
FROM
(WITH RECURSIVE Message_CTE(MessageId, RootPostId, RootPostLanguage, ContainerForumId, ParentMessageId) AS (
    -- first half of the union: Comments attaching directly to the existing tree
    SELECT
        C.MessageId AS MessageId,
        M.RootPostId AS RootPostId,
        M.RootPostLanguage AS RootPostLanguage,
        M.ContainerForumId AS ContainerForumId,
        C.ParentMessageId AS ParentMessageId
    FROM Message C
    JOIN Message M
        ON M.MessageId = C.ParentMessageId
    WHERE C.RootPostId = -1 AND M.RootPostId <> -1
    UNION ALL
    -- second half of the union: Comments attaching newly inserted Comments
    SELECT
        C.MessageId AS MessageId,
        Message_CTE.RootPostId AS RootPostId,
        Message_CTE.RootPostLanguage AS RootPostLanguage,
        Message_CTE.ContainerForumId AS ContainerForumId,
        C.ParentMessageId AS ParentMessageId
    FROM Message C
    JOIN Message_CTE
        ON FORCEORDER(C.ParentMessageId = Message_CTE.MessageId)
    WHERE C.RootPostId = -1
) SELECT * FROM Message_CTE) o
WHERE res.RootPostId = -1 AND res.MessageId = o.MessageId
;