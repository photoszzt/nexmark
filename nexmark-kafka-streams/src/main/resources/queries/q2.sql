-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
--
-- In original Nexmark queries, Query2 is as following (in CQL syntax):
--
--   SELECT Rstream(auction, price)
--   FROM Bid [NOW]
--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
--
-- However, that query will only yield a few hundred results over event streams of arbitrary size.
-- To make it more interesting we instead choose bids for every 123'th auction.
-- -------------------------------------------------------------------------------------------------

CREATE STREAM sink_q2 AS
    SELECT 
        data_gen.bid->auction, 
        data_gen.bid->price
    FROM data_gen 
    WHERE data_gen.type = 2 and data_gen.bid->auction % 123 = 0
    EMIT CHANGES;