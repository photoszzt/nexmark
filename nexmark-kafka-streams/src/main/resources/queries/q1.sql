-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE STREAM sink_q1 AS
    SELECT 
        data_gen.bid->auction, 
        data_gen.bid->bidder, 
        0.908 * data_gen.bid->price as price, 
        data_gen.bid->dateTime, 
        data_gen.bid->extra
    FROM data_gen
    where data_gen.type = 2
    EMIT CHANGES;