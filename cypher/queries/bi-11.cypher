// Q11. Friend triangles
/*
:param [{ country, startDate, endDate }] => { RETURN
  'Belarus' AS country,
  datetime('2010-06-01') AS startDate,
  datetime('2010-07-01') AS endDate
}
*/
MATCH (country:Country {name: $country})
MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (c:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (a)-[k1:KNOWS]-(b)-[k2:KNOWS]-(c)-[k3:KNOWS]-(a)
WHERE a.id < b.id
  AND b.id < c.id
  AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
  AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
  AND $startDate <= k3.creationDate AND k3.creationDate <= $endDate
WITH DISTINCT a, b, c
RETURN count(*) AS count
