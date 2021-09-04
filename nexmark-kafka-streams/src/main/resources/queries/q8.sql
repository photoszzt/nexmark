-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

create table person_just_enter as
    SELECT P.id, P.name, 
        WINDOWSTART as starttime, WINDOWEND as endtime
        FROM person P
        WINDOW TUMBLING (SIZE 10 SECONDS) 
        GROUP BY P.id, P.name
        emit changes;

create table recent_auction as 
    SELECT A.seller, WINDOWSTART AS starttime,
        WINDOWEND AS endtime
        FROM auction A
        WINDOW TUMBLING (SIZE 10 SECONDS)
        GROUP BY A.seller
        emit changes;

CREATE TABLE sink_q8 AS
    SELECT P.id, P.name, P.starttime
        FROM person_just_enter P
        JOIN recent_auction A
        ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime
        EMIT CHANGES;