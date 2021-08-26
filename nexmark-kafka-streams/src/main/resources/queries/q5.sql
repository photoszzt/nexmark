-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink AS
  SELECT AuctionBids.auction, AuctionBids.num
  FROM (
    SELECT
        B1.auction, count(*) AS num,
        WINDOWSTART AS starttime,
        WINDOWEND AS endtime
    FROM bid B1
    WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 2 SECONDS)
    GROUP BY B1.auction
  ) AS AuctionBids
  JOIN (
    SELECT
      max(CountBids.num) AS maxn,
      CountBids.starttime,
      CountBids.endtime,
    FROM (
        SELECT
          count(*) AS num,
          WINDOWSTART AS starttime,
          WINDOWEND AS endtime
        FROM bid B2
        WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 2 SECONDS)
        GROUP BY B2.auction
    ) AS CountBids
    GROUP BY CountBids.starttime, CountBids.endtime
  ) AS MaxBids
  ON AuctionBids.starttime = MaxBids.starttime AND
     AuctionBids.endtime = MaxBids.endtime AND
     AuctionBids.num >= MaxBids.maxn
  EMIT CHANGES;