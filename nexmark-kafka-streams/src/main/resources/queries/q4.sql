-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the wining bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------
CREATE TABLE discard_sink AS
    SELECT Q.category, AVG(Q.final)
    FROM (
        SELECT MAX(B.price) AS final, A.category
        FROM auction A, bid B
        WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
        GROUP BY A.id, A.category
    ) Q
    GROUP BY Q.category EMIT CHANGES;