-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink_q3 AS
    SELECT A.seller, P.name, P.city, P.state, A.id
    FROM auction AS A INNER JOIN person as P on A.seller = P.id
    WHERE A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')
    EMIT CHANGES;