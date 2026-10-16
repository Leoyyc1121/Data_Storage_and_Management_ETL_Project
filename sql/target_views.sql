-- SQLite 3.38+; JSON1 is bundled with Python 3.11's SQLite runtime.
CREATE INDEX IF NOT EXISTS review_product_index ON raw_reviews(product_id);
CREATE INDEX IF NOT EXISTS review_logical_index ON raw_reviews(logical_review_id);
CREATE INDEX IF NOT EXISTS product_movie_work_index ON product_movie(movie_id);
CREATE INDEX IF NOT EXISTS lineage_parent_index ON lineage_edges(parent_kind,parent_id);
CREATE VIEW IF NOT EXISTS field_lineage AS
SELECT s.entity_type,s.entity_id,s.field,s.value AS final_value,s.status AS final_status,s.reason AS selection_rationale,
 m.status AS candidate_status,m.reason AS candidate_rationale,a.assertion_id,a.raw_value,a.normalized_value,
 a.field_locator,a.reason AS extraction_semantics,a.operation_id,o.rule_id,o.run_id,r.started_at AS operation_time,
 COALESCE(a.source_record_id,rs.source_review_id) AS source_record_id,COALESCE(e.source_id,CASE WHEN rs.source_review_id IS NOT NULL THEN 'snap_reviews' END) AS source_id,
 COALESCE(e.archive_sha256,rv.source_file_sha256) AS archive_sha256,COALESCE(e.record_sequence,rv.record_sequence) AS record_sequence,
 COALESCE(e.byte_start,rv.byte_start) AS byte_start,COALESCE(e.byte_end,rv.byte_end) AS byte_end,
 COALESCE(e.raw_sha256,rv.raw_record_sha256) AS raw_sha256,e.raw_record,COALESCE(e.url,sr.source_url) AS url,COALESCE(e.acquired_at,sr.downloaded_at) AS acquired_at
FROM field_selections s LEFT JOIN selection_members m USING(selection_id) LEFT JOIN assertions a USING(assertion_id)
LEFT JOIN operations o ON a.operation_id=o.operation_id LEFT JOIN etl_runs r ON o.run_id=r.run_id
LEFT JOIN source_records e ON a.source_record_id=e.source_record_id
LEFT JOIN review_assertion_sources rs ON a.assertion_id=rs.assertion_id LEFT JOIN raw_reviews rv ON rs.source_review_id=rv.source_review_id
LEFT JOIN review_source_registry sr ON rv.source_file_sha256=sr.source_sha256;
CREATE VIEW IF NOT EXISTS review_associations AS
SELECT r.source_review_id,r.record_sequence,r.logical_review_id,r.product_id AS asin,e.classification,e.mapping_status,
 m.movie_id,m.relationship,w.identity_status FROM raw_reviews r JOIN editions e ON r.product_id=e.asin
LEFT JOIN product_movie m ON e.asin=m.asin LEFT JOIN movies w ON m.movie_id=w.movie_id;
CREATE VIEW IF NOT EXISTS review_field_lineage AS
SELECT r.source_review_id,r.record_sequence,f.field,f.raw_prefix,f.raw_column,f.normalized_column,
 CASE f.raw_column WHEN 'product_id' THEN r.product_id WHEN 'reviewer_id' THEN r.reviewer_id WHEN 'profile_name' THEN r.profile_name WHEN 'summary' THEN r.summary WHEN 'review_text' THEN r.review_text WHEN 'score_raw' THEN r.score_raw WHEN 'helpfulness_raw' THEN r.helpfulness_raw WHEN 'review_time_raw' THEN r.review_time_raw END AS raw_value,
 CASE f.normalized_column WHEN 'product_id' THEN r.product_id WHEN 'reviewer_id' THEN r.reviewer_id WHEN 'profile_name' THEN r.profile_name WHEN 'summary_clean' THEN r.summary_clean WHEN 'review_text_clean' THEN r.review_text_clean WHEN 'score' THEN CAST(r.score AS TEXT) WHEN 'helpful_votes' THEN CAST(r.helpful_votes AS TEXT) WHEN 'total_votes' THEN CAST(r.total_votes AS TEXT) WHEN 'helpfulness_ratio' THEN CAST(r.helpfulness_ratio AS TEXT) WHEN 'review_time_utc' THEN r.review_time_utc END AS normalized_value,
 r.source_file_sha256,r.raw_record_sha256,r.byte_start,r.byte_end,r.start_line,r.end_line,
 r.run_id AS extraction_run_id,r.rule_version AS extraction_rule_id,r.parser_version,
 r.clean_run_id,r.clean_rule_version,f.operation_type,f.semantics,
 json_array('review_raw:'||r.source_review_id,'parse:'||r.run_id,'clean:'||r.clean_run_id) AS processing_parent_chain,
 r.raw_record_base64,s.source_url,s.source_path,s.downloaded_at,s.missing_time_reason,
 cr.started_at AS cleaning_started_at,cr.finished_at AS cleaning_finished_at
FROM raw_reviews r CROSS JOIN review_field_rules f LEFT JOIN review_source_registry s ON r.source_file_sha256=s.source_sha256
LEFT JOIN review_runs cr ON r.clean_run_id=cr.run_id;
CREATE VIEW IF NOT EXISTS product_review_statistics AS
SELECT l.asin,count(*) AS logical_review_count,count(r.score) AS valid_score_denominator,avg(r.score) AS mean_score,
 (SELECT count(*) FROM raw_reviews raw WHERE raw.product_id=l.asin) AS raw_review_count
FROM logical_reviews l JOIN raw_reviews r ON l.representative_sequence=r.record_sequence GROUP BY l.asin;
CREATE VIEW IF NOT EXISTS movie_review_statistics AS
WITH relevant_products AS (SELECT DISTINCT asin FROM product_movie WHERE relationship='single_work'),
per_product AS (
 SELECT p.asin,count(*) AS logical_review_count,count(r.score) AS valid_score_denominator,avg(r.score) AS mean_score,
 (SELECT count(*) FROM raw_reviews raw WHERE raw.product_id=p.asin) AS raw_review_count
 FROM relevant_products p CROSS JOIN logical_reviews l ON l.asin=p.asin
 JOIN raw_reviews r ON r.record_sequence=l.representative_sequence GROUP BY p.asin
)
SELECT m.movie_id,w.identity_status,sum(p.raw_review_count) AS raw_review_count,
 sum(p.logical_review_count) AS logical_review_count,sum(p.valid_score_denominator) AS valid_score_denominator,
 sum(p.mean_score*p.valid_score_denominator)/NULLIF(sum(p.valid_score_denominator),0) AS mean_score
FROM product_movie m JOIN movies w USING(movie_id) JOIN per_product p ON m.asin=p.asin
WHERE m.relationship='single_work' GROUP BY m.movie_id,w.identity_status;
CREATE VIEW IF NOT EXISTS work_review_observations AS
WITH ranked AS (
 SELECT m.movie_id,r.source_review_id,r.product_id,r.review_time_utc,r.review_time_raw,r.source_file_sha256,
 r.raw_record_sha256,r.record_sequence,r.byte_start,r.byte_end,r.clean_rule_version,r.clean_run_id,
 row_number() OVER(PARTITION BY m.movie_id ORDER BY CAST(r.review_time_raw AS INTEGER),r.record_sequence) AS position
 FROM product_movie m CROSS JOIN raw_reviews r INDEXED BY review_product_index ON r.product_id=m.asin
 WHERE m.relationship='single_work' AND r.review_time_utc IS NOT NULL
)
SELECT movie_id,review_time_utc AS earliest_review_time,source_review_id,product_id AS asin,review_time_raw,
 source_file_sha256,raw_record_sha256,record_sequence,byte_start,byte_end,clean_rule_version,clean_run_id,
 'earliest_valid_observed_review_not_release_date' AS observation_semantics,
 NULL AS inferred_release_bound,'preorder_and_work_identity_conditions_not_verified' AS inference_reason
FROM ranked WHERE position=1;
