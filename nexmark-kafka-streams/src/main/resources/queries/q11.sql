-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink AS
  SELECT
    B.bidder,
    count(*) as bid_count,
    WINDOWSTART as starttime,
    WINDOWEND as endtime
  FROM bid B
  WINDOW SESSION (SIZE 10 SECONDS)
  EMIT CHANGES;