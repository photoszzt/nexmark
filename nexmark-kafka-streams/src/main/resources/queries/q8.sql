-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink AS
  SELECT P.id, P.name, P.starttime
  FROM (
    SELECT P.id, P.name, 
    WINDOWSTART as starttime, WINDOWEND as endtime
    FROM person P
    WINDOW TUMBLING (SIZE 10 SECONDS) 
    GROUP BY P.id, P.name
  ) P
  JOIN (
    SELECT A.seller, WINDOWSTART AS starttime,
    WINDOWEND AS endtime
    FROM auction A
    WINDOW TUMBLING (SIZE 10 SECONDS)
    GROUP BY A.seller
  ) A
  ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime
  EMIT CHANGES;