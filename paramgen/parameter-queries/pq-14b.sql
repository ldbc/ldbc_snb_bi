SELECT
    country1Name AS 'country1:STRING',
    country2Name AS 'country2:STRING'
FROM countryPairsNumFriends
ORDER BY frequency ASC
LIMIT 10
