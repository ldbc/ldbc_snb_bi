COPY Organisation              FROM '${PATHVAR}/static/Organisation${POSTFIX}'                 (DELIMITER '|' ${HEADER});
COPY Place                     FROM '${PATHVAR}/static/Place${POSTFIX}'                        (DELIMITER '|' ${HEADER});
COPY Tag                       FROM '${PATHVAR}/static/Tag${POSTFIX}'                          (DELIMITER '|' ${HEADER});
COPY TagClass                  FROM '${PATHVAR}/static/TagClass${POSTFIX}'                     (DELIMITER '|' ${HEADER});
