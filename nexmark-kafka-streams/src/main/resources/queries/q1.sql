-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink AS
SELECT auction, bidder, 0.908 * price as price, dataTime, extra
FROM data_gen.bid EMIT CHANGES;