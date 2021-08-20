-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Kafka SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------
CREATE TABLE discard_sink AS
    SELECT auction, bidder, price, dataTime, extra from data_gen.bid
    EMIT CHANGES;