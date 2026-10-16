-- Each query is independently executable in the final SQLite database.
-- 1 Work editions. Supply :movie_id; NULL displays a small sample.
SELECT m.movie_id,e.asin,e.title,e.medium,e.language,e.subtitles,e.region_code,e.mapping_status
FROM product_movie m JOIN editions e USING(asin) WHERE :movie_id IS NULL OR m.movie_id=:movie_id ORDER BY m.movie_id,e.asin LIMIT CASE WHEN :movie_id IS NULL THEN 30 ELSE -1 END;

-- 2 Director works and exact sources. Supply :person_id; roles remain separate.
SELECT DISTINCT p.person_id,p.canonical_name,c.movie_id,w.canonical_title,c.raw_name,s.source_id,s.archive_sha256,s.record_sequence,c.assertion_id
FROM credits c JOIN people p USING(person_id) JOIN movies w USING(movie_id) JOIN source_records s USING(source_record_id)
WHERE c.role='director' AND (:person_id IS NULL OR c.person_id=:person_id) ORDER BY p.person_id,c.movie_id LIMIT CASE WHEN :person_id IS NULL THEN 30 ELSE -1 END;

-- 3 Missing and inferred dates. No year-only -> January1 conversion.
SELECT w.movie_id,w.canonical_title,r.release_year,r.release_date,r.date_precision,r.region,r.release_type,
 r.year_reason,r.date_reason,r.inferred_upper_bound,r.inference_reason,w.identity_status
FROM release_info r JOIN movies w USING(movie_id) WHERE r.release_date IS NULL OR r.inferred_upper_bound IS NOT NULL ORDER BY w.movie_id LIMIT 30;

-- 4 Conflicts, all candidates, selections and reasons. Actual full data may have zero equal-authority conflicts.
SELECT s.entity_type,s.entity_id,s.field,s.status,s.reason,s.value,a.raw_value,a.normalized_value,m.status AS candidate_status,a.source_record_id
FROM field_selections s LEFT JOIN selection_members m USING(selection_id) LEFT JOIN assertions a USING(assertion_id)
WHERE s.status='unresolved' OR (s.entity_type='movie' AND s.field='title') ORDER BY s.status,s.entity_id LIMIT 30;

-- 5 Review counts/scores. Raw preserved; only byte-identical within-ASIN records dedup;
-- denominator valid logical-review scores, not people or source assertions;
-- compilations product-only; across-edition candidates remain distinct; show provisional identity.
SELECT * FROM movie_review_statistics ORDER BY movie_id LIMIT 30;

-- 6 Required field lineage. Defaults select one populated movie field without revealing aggregate content in Git.
SELECT entity_id,field,final_value,final_status,candidate_status,raw_value,normalized_value,source_record_id,archive_sha256,
 record_sequence,byte_start,byte_end,field_locator,selection_rationale,rule_id,run_id
FROM field_lineage WHERE entity_type='movie' AND (:movie_id IS NULL OR entity_id=:movie_id) AND (:field_name IS NULL OR field=:field_name) LIMIT CASE WHEN :movie_id IS NULL THEN 30 ELSE -1 END;

-- 7 Required ASIN/review mapping query; input source_review_id or ASIN.
SELECT * FROM review_associations WHERE (:source_review_id IS NULL OR source_review_id=:source_review_id)
AND (:asin IS NULL OR asin=:asin) LIMIT CASE WHEN :source_review_id IS NULL AND :asin IS NULL THEN 30 ELSE -1 END;

-- 8 Required source impact query. Returns BOTH selected and unselected assertions.
WITH RECURSIVE affected(kind,id) AS (
 SELECT 'source',COALESCE(:source_record_id,(SELECT parent_id FROM lineage_edges WHERE parent_kind='source' ORDER BY parent_id LIMIT 1))
 UNION SELECT e.child_kind,e.child_id FROM lineage_edges e JOIN affected a ON e.parent_kind=a.kind AND e.parent_id=a.id
)
SELECT DISTINCT s.entity_type,s.entity_id,s.field,s.status,s.reason
FROM affected a JOIN field_selections s ON a.kind='selection' AND s.selection_id=a.id LIMIT CASE WHEN :source_record_id IS NULL THEN 30 ELSE -1 END;

-- 9 Required person trace: original credits, alias history, work/role and evidence.
SELECT p.person_id,p.nconst,p.canonical_name,p.identity_status,a.alias_id,a.raw_name,a.normalized_name,
 a.movie_id,a.role,c.credit_order,c.scope,a.assertion_id,a.source_record_id,s.archive_sha256,s.record_sequence,
 s.byte_start,s.byte_end,d.status AS resolution_status,d.reason AS resolution_reason,d.evidence AS resolution_evidence,
 o.rule_id,o.run_id,r.started_at AS operation_time
FROM people p JOIN person_aliases a USING(person_id)
JOIN source_records s ON a.source_record_id=s.source_record_id
JOIN assertions x ON a.assertion_id=x.assertion_id JOIN operations o USING(operation_id) JOIN etl_runs r USING(run_id)
LEFT JOIN credits c ON c.assertion_id=a.assertion_id
LEFT JOIN person_decisions d ON d.movie_id=a.movie_id AND d.role=a.role AND d.raw_name=a.raw_name AND d.person_id=a.person_id
 AND json_extract(d.evidence,'$.source_record_id')=a.source_record_id
 AND json_extract(d.evidence,'$.field_locator')=x.field_locator
WHERE :person_id IS NULL OR p.person_id=:person_id
ORDER BY p.person_id,a.movie_id,a.alias_id LIMIT CASE WHEN :person_id IS NULL THEN 30 ELSE -1 END;

-- 10 Earliest observed review is NOT an inferred or verified release date.
SELECT * FROM work_review_observations WHERE :movie_id IS NULL OR movie_id=:movie_id
ORDER BY movie_id LIMIT CASE WHEN :movie_id IS NULL THEN 30 ELSE -1 END;
