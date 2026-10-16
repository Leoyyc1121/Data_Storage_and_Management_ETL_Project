-- SQLite read-only inspection queries for the Task4 integration checkpoint.
-- This is not the final PostgreSQL schema or the project's final five demo queries.

-- 1. All original products of supported work groups; display labels are provisional.
SELECT w.movie_id, w.status, w.display_title, pm.asin, p.classification,
       json_extract(p.profile, '$.raw_titles') AS original_titles,
       p.review_count, pm.relationship
FROM works w JOIN product_movie pm USING(movie_id) JOIN products p USING(asin)
WHERE w.status='multifield_group'
ORDER BY w.movie_id, pm.asin;

-- 2. Pending/conflicting pairs, preserving both source assertion payloads.
SELECT pair_id, left_asin, right_asin, proposed_decision, final_decision,
       rule_id, rule_version, evidence
FROM pairs WHERE final_decision IN ('manual_review','unresolved')
ORDER BY pair_id;

-- 3. Reject/cluster-block evidence; a blocked edge must not create a transitive merge.
SELECT p.pair_id, p.left_asin, p.right_asin, p.final_decision, p.rule_id,
       c.left_asin AS checked_left, c.right_asin AS checked_right,
       c.decision AS cross_pair_decision, c.evidence
FROM pairs p LEFT JOIN cluster_checks c ON c.merge_pair_id=p.pair_id
WHERE p.final_decision='reject' OR p.rule_id='transitive_cluster_cross_pair_not_supported'
ORDER BY p.pair_id, c.left_asin, c.right_asin;

-- 4. Full ASIN scope and review references: sum these product rows, not joined cast/editions.
SELECT p.classification, s.mapping_status, COUNT(*) AS products,
       SUM(p.review_count) AS original_review_references
FROM products p JOIN product_status s USING(asin)
GROUP BY p.classification, s.mapping_status ORDER BY p.classification, s.mapping_status;

-- 5. Compilation reviews remain on original products; no per-contained-movie replication.
SELECT p.asin, p.review_count, s.mapping_status, s.reason,
       (SELECT COUNT(*) FROM product_movie pm WHERE pm.asin=p.asin) AS supported_work_memberships
FROM products p JOIN product_status s USING(asin)
WHERE p.classification='compilation' ORDER BY p.asin;

-- 6. Independent evaluation reservation; labels are kept in a separate HUMAN-reviewed file.
SELECT stratum, partition, sampling_version, COUNT(*) AS reserved_pairs
FROM evaluation_pairs GROUP BY stratum, partition, sampling_version;

-- 7. Reversible versioned mapping events and run history.
SELECT event_id, rule_version, run_id, event FROM mapping_history ORDER BY event_id;
SELECT run_id, phase, status, started_at, finished_at FROM runs ORDER BY started_at;
