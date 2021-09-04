-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the wining bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------

create table Q as 
    SELECT A.id as id, MAX(B.price) AS final, A.category as category
        FROM auction A inner join bid B on A.id = B.auction
        WHERE B.dateTime BETWEEN A.dateTime AND A.expires
        GROUP BY A.id, A.category;

CREATE TABLE sink_q4 AS
    SELECT Q.category, AVG(Q.final)
    FROM Q
    GROUP BY Q.category EMIT CHANGES;