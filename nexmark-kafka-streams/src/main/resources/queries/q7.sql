-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

create table B1 as
    SELECT MAX(B1.price) AS maxprice, ROWTIME as dateTime
    FROM bid B1
    WINDOW TUMBLING (SIZE 10 SECONDS)
    emit changes;

CREATE TABLE sink_q7 AS
  SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
  from bid B
  JOIN B1
  ON B.price = B1.maxprice
  WHERE B.dateTime BETWEEN TIMESTAMPSUB(B1.dateTime, 10 SECONDS) and B1.dateTime
  EMIT CHANGES; 