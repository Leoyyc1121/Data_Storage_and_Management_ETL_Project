"""Task4 synthetic rules/recovery tests; never human labels or factual sources."""
from contextlib import closing
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"src/transform"))
import resolve_movies as resolution
import test_task2_metadata as fixtures
import acquire_product_metadata as metadata
import classify_products
from acquire_products import export as export_task2, save_json
from evaluate_task4 import evaluate, metrics
from verify_task4 import verify


def profile(asin, *, title="synthetic work", classification="movie", values=None, variants=None, plots=None):
    return dict(asin=asin,titles=[title],classification=classification,eligible=classification in {"movie","awaiting_confirmation"},
        values=values or {},variants=variants or [],long_plots=plots or [],plot_tokens=["synthetic","story"],records=[{"source_record_id":"synthetic:"+asin}])


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.rules=resolution.load_rules(resolution.ROOT/"config/task4.rules.json")

    def decision(self,a,b):
        return resolution.compare(a,b,self.rules)

    def test_title_only_never_auto(self):
        self.assertEqual(self.decision(profile("A"),profile("B"))["proposed_decision"],"manual_review")

    def test_missing_year_not_positive_evidence(self):
        a,b=profile("A"),profile("B")
        d=self.decision(a,b)
        self.assertEqual(d["facts"]["shared_work_years"],[])
        self.assertNotEqual(d["proposed_decision"],"auto_merge")

    def test_shared_year_alone_insufficient(self):
        a,b=profile("A",values={"work_years":[2000]}),profile("B",values={"work_years":[2000]})
        self.assertNotEqual(self.decision(a,b)["proposed_decision"],"auto_merge")

    def test_year_director_multifield_supported(self):
        values={"work_years":[2000],"directors":["synthetic director"]}
        self.assertEqual(self.decision(profile("A",values=values),profile("B",values=values))["proposed_decision"],"auto_merge")

    def test_director_two_credited_stars_supported_without_invented_year(self):
        values={"directors":["synthetic director"],"credited_stars":["actor one","actor two"]}
        self.assertEqual(self.decision(profile("A",values=values),profile("B",values=values))["proposed_decision"],"auto_merge")

    def test_long_plot_alone_needs_other_work_evidence(self):
        a,b=profile("A",plots=["synthetic-long-plot"]),profile("B",plots=["synthetic-long-plot"])
        self.assertNotEqual(self.decision(a,b)["proposed_decision"],"auto_merge")
        for p in (a,b):p["values"]["actor_context"]=["actor one","actor two"]
        self.assertEqual(self.decision(a,b)["proposed_decision"],"auto_merge")

    def test_unverified_imdb_mentions_not_identity(self):
        values={"external_id_mentions":["tt0000001"]}
        self.assertNotEqual(self.decision(profile("A",values=values),profile("B",values=values))["proposed_decision"],"auto_merge")

    def test_same_title_remake_separated(self):
        a=profile("A",values={"work_years":[1973],"directors":["original director"]})
        b=profile("B",values={"work_years":[2010],"directors":["different director"]})
        self.assertEqual(self.decision(a,b)["proposed_decision"],"reject")

    def test_conflicting_year_without_more_context_requires_review(self):
        a,b=profile("A",values={"work_years":[1973]}),profile("B",values={"work_years":[2010]})
        self.assertEqual(self.decision(a,b)["proposed_decision"],"manual_review")

    def test_within_product_year_conflict_not_selected(self):
        a,b=profile("A",values={"work_years":[1999,2000]}),profile("B",values={"work_years":[2000]})
        self.assertEqual(self.decision(a,b)["rule_id"],"within_product_year_conflict")

    def test_runtime_difference_and_cuts_require_review(self):
        a=profile("A",values={"work_years":[2000],"directors":["director one"],"runtimes":[90]})
        b=deepcopy(a);b["asin"]="B";b["values"]["runtimes"]=[120]
        self.assertEqual(self.decision(a,b)["rule_id"],"runtime_or_cut_difference_requires_review")
        b["values"]["runtimes"]=[90];b["variants"]=["director_cut"]
        self.assertEqual(self.decision(a,b)["rule_id"],"cut_restoration_or_version_requires_review")

    def test_sequel_digits_preserved_and_not_merged(self):
        self.assertEqual(self.decision(profile("A",title="synthetic saga ii"),profile("B",title="synthetic saga iii"))["rule_id"],"different_sequel_numerals")
        self.assertEqual(self.decision(profile("A",title="synthetic saga"),profile("B",title="synthetic saga 2"))["proposed_decision"],"manual_review")

    def test_omitted_number_and_equivalent_roman_arabic_are_not_rejected(self):
        self.assertEqual(self.decision(profile("A",title="synthetic bloodline"),profile("B",title="synthetic iv bloodline"))["rule_id"],"sequence_number_omitted_requires_review")
        self.assertNotEqual(self.decision(profile("A",title="synthetic ii"),profile("B",title="synthetic 2"))["proposed_decision"],"reject")

    def test_explicit_starring_continuation_retains_all_names(self):
        raw=json.dumps(dict(asin="B000000001",title="Synthetic",description=["Starring Alex Actor<br>Beth Player<br>Carl Star<br>Writer David Writer"])).encode()
        row=dict(raw_record=raw,source_id="synthetic",archive_sha256="0"*64,record_sequence=1,byte_start=0,byte_end=len(raw),raw_record_sha256=hashlib.sha256(raw).hexdigest())
        result=resolution.extract_record(row,self.rules)
        self.assertEqual(result["values"]["credited_stars"],["alex actor","beth player","carl star"])

    def test_normalization_preserves_original_work_discriminators(self):
        norm=resolution.normalize_title
        self.assertEqual(norm("The Film II [DVD] (1999)"),"the film ii")
        self.assertEqual(norm("1984"),"1984")
        self.assertIn("ii",norm("Film II - 2-disc DVD"))
        self.assertIn("original subtitle",norm("Film (Original Subtitle)"))

    def test_alias_candidate_only_not_external_fact(self):
        self.assertIn("the scar",resolution.title_forms("The Scar aka Hollow Triumph VHS"))
        self.assertIn("hollow triumph",resolution.title_forms("The Scar aka Hollow Triumph VHS"))

    def test_bare_edition_descriptions_removed_but_cut_flag_not_forgotten(self):
        self.assertEqual(resolution.normalize_title("Synthetic Film - Special Edition DVD"),"synthetic film")
        self.assertEqual(resolution.normalize_title("Synthetic Film Director's Cut"),"synthetic film")

    def test_language_aliases_preserve_raw_and_do_not_prove_work_identity(self):
        item=dict(asin="B000000001",title="Synthetic DVD",description=[],details={"Language:":"English, Français", "Subtitles:":"Farsi"})
        raw=json.dumps(item).encode()
        row=dict(raw_record=raw,source_id="synthetic",archive_sha256="0"*64,record_sequence=1,byte_start=0,byte_end=len(raw),raw_record_sha256=hashlib.sha256(raw).hexdigest())
        result=resolution.extract_record(row,self.rules)
        language=next(value for value in result["edition_assertions"] if value["field"]=="language")
        self.assertEqual(language["raw"],"English, Français")
        self.assertEqual(language["language_tags"],["en","fr"])
        self.assertEqual(language["status"],"candidate_not_selected")

    def test_television_compilation_nonmovie_never_merge(self):
        a=profile("A",values={"work_years":[2000],"directors":["same director"]})
        for label in ("television_series","compilation","non_movie","concert"):
            b=profile("B",classification=label,values=a["values"])
            self.assertEqual(self.decision(a,b)["proposed_decision"],"reject")

    def test_context_actor_not_final_starring(self):
        raw=json.dumps(dict(asin="B000000001",title="Synthetic DVD",description=["Hero (Alex Actor) meets Friend (Beth Player). Director of Photography Charles Camera."],brand="Not Director")).encode()
        row=dict(raw_record=raw,source_id="synthetic",archive_sha256="0"*64,record_sequence=1,byte_start=0,byte_end=len(raw),raw_record_sha256=hashlib.sha256(raw).hexdigest())
        p=resolution.extract_record(row,self.rules)
        self.assertEqual(p["values"]["actor_context"],["alex actor","beth player"])
        self.assertNotIn("directors",p["values"])
        self.assertNotIn("credited_stars",p["values"])

    def test_name_boundary_and_role_label(self):
        self.assertEqual(resolution.name_prefix("Steven Spielberg and nominated for awards"),"steven spielberg")
        self.assertEqual(resolution.name_prefix("Jane Smith. This film"),"jane smith")
        self.assertIsNone(resolution.name_prefix("of Photography Jane Smith"))

    def test_title_year_not_final_work_year(self):
        item=dict(asin="B000000001",title="Synthetic (1999)",description=["Released on DVD in 2001. This 1980 film tells a story."])
        raw=json.dumps(item).encode()
        row=dict(raw_record=raw,source_id="synthetic",archive_sha256="0"*64,record_sequence=1,byte_start=0,byte_end=len(raw),raw_record_sha256=hashlib.sha256(raw).hexdigest())
        result=resolution.extract_record(row,self.rules)
        self.assertEqual(result["values"]["work_years"],[1980])

    def test_transitive_bridge_conflict_prevents_cluster(self):
        a=profile("A",values={"directors":["ann director"],"credited_stars":["star one","star two"]})
        b=profile("B",values={"directors":["ann director","beth director"],"credited_stars":["star one","star two"]})
        c=profile("C",values={"directors":["beth director"],"credited_stars":["star one","star two"]})
        pairs=[("AB","A","B",self.decision(a,b)),("BC","B","C",self.decision(b,c))]
        groups,checks,events,blocked=resolution.complete_link_clusters({"A":a,"B":b,"C":c},pairs,self.rules)
        self.assertEqual(len(groups),2)
        self.assertIn("BC",blocked)
        self.assertEqual(len(events),1)

    def test_candidate_requires_movie_seed_but_can_match_pending(self):
        a,b=profile("A"),profile("B",classification="awaiting_confirmation")
        pairs,_=resolution.generate_candidates({"A":a,"B":b},self.rules)
        self.assertEqual(len(pairs),1)
        a["classification"]="awaiting_confirmation"
        self.assertEqual(resolution.generate_candidates({"A":a,"B":b},self.rules)[0],[])

    def test_metrics_no_fabricated_perfect_zero_denominator(self):
        result=metrics([True,False],[False,False])
        self.assertIsNone(result["precision"])
        self.assertEqual(result["recall"],0)
        self.assertIsNone(result["f1"])
        self.assertEqual(metrics([True,False],[True,False])["f1"],1.0)

    def test_metrics_rejects_unaligned_labels(self):
        with self.assertRaises(ValueError):metrics([True,False],[True])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.MetadataTests.setUp(self)
        desc="This 2000 film is directed by Jane Smith. A synthetic story only for testing.<br>Starring: Alex Actor, Beth Player"
        self.lines=[json.dumps(dict(asin=asin,title=title,category=["Movies"],description=[desc])).encode()+b"\n" for asin,title in ((fixtures.ASIN,"Synthetic Work DVD"),(fixtures.OTHER,"Synthetic Work Blu-ray"))]
        self.archive.write_bytes(gzip.compress(b"".join(self.lines),mtime=0))
        self.registration["archive_sha256"]=resolution.hash_file(self.archive)
        metadata.import_archive(self.metadata_db,self.registration,self.spec,self.fingerprint)
        metadata.export_metadata(self.metadata_db,self.config)
        export_task2(self.db,self.config)
        task2_path=self.root/"task2.json";save_json(task2_path,self.config)
        self.task3=dict(task2_config=str(task2_path),rules_file=str(resolution.ROOT/"config/task3.rules.json"),work_dir=str(self.root/"task3"),report_dir=str(self.root/"reports3"),batch_size=1)
        classify_products.run(self.task3)
        task3_path=self.root/"task3.json";save_json(task3_path,self.task3)
        self.task4=dict(task3_config=str(task3_path),rules_file=str(resolution.ROOT/"config/task4.rules.json"),work_dir=str(self.root/"task4"),report_dir=str(self.root/"reports4"),batch_size=1,evaluation_pairs=120,development_pairs=[])

    def test_full_pipeline_merges_only_multifield_and_retains_products_reviews(self):
        status,stats=resolution.run(self.task4)
        self.assertEqual(status,"complete")
        self.assertEqual((stats["products"],stats["reviews_retained"]),(2,3))
        self.assertEqual((stats["before_provisional_movie_seeds"],stats["after_work_records"]),(2,1))
        self.assertEqual(stats["decisions"]["auto_merge"],1)
        self.assertEqual(verify(self.task4)["work_records"],1)
        with closing(sqlite3.connect(Path(self.task4["work_dir"])/"resolution.sqlite3")) as db:
            mid=db.execute("SELECT movie_id FROM works").fetchone()[0]
            self.assertTrue(mid.startswith("mw_"))
            self.assertNotIn(mid,(fixtures.ASIN,fixtures.OTHER))

    def test_repeat_output_hashes_identical(self):
        _,a=resolution.run(self.task4)
        _,b=resolution.run(self.task4)
        self.assertEqual(a["outputs"],b["outputs"])
        self.assertEqual(a["evaluation"]["queue_sha256"],b["evaluation"]["queue_sha256"])

    def test_profile_interruption_resume(self):
        status,stats=resolution.run(self.task4,stop_after_batches=1)
        self.assertEqual((status,stats),("interrupted",None))
        status,stats=resolution.run(self.task4)
        self.assertEqual((status,stats["products"],stats["after_work_records"]),("complete",2,1))

    def test_pair_interruption_resume(self):
        status,_=resolution.run(self.task4,stop_after_batches=1,stop_phase="pairs")
        self.assertEqual(status,"interrupted")
        _,stats=resolution.run(self.task4)
        self.assertEqual(stats["candidate_pairs"],1)
        self.assertEqual(verify(self.task4)["candidate_pairs_replayed"],1)

    def test_input_rule_change_rejected_and_old_decisions_retained(self):
        resolution.run(self.task4)
        rule=json.loads(Path(self.task4["rules_file"]).read_text());rule["version"]="synthetic-new-version"
        path=self.root/"new_rules.json";save_json(path,rule)
        with self.assertRaises(ValueError):resolution.run(dict(self.task4,rules_file=str(path)))

    def test_unlabeled_queue_never_becomes_fake_accuracy(self):
        _,stats=resolution.run(self.task4)
        self.assertEqual(stats["evaluation"]["manually_labeled_pairs"],0)
        self.assertIsNone(stats["evaluation"]["precision"])
        with self.assertRaises(ValueError):evaluate(self.task4,stats["evaluation"]["queue_path"])

    def test_evaluation_development_pairs_disjoint(self):
        self.task4["development_pairs"]=[[fixtures.ASIN,fixtures.OTHER]]
        _,stats=resolution.run(self.task4)
        self.assertEqual(stats["evaluation"]["queue_pairs"],0)
        self.assertEqual(verify(self.task4)["evaluation_development_overlap"],0)

    def test_mapping_tamper_detected_even_after_export(self):
        resolution.run(self.task4)
        _,reg=resolution.input_registration(self.task4,verify_input=False)
        with closing(resolution.open_db(self.task4,reg)) as db:
            with db:db.execute("DELETE FROM product_movie WHERE asin=?",(fixtures.OTHER,))
            resolution.export(db,self.task4,resolution.load_rules(self.task4["rules_file"]),reg)
        with self.assertRaisesRegex(ValueError,"mapping"):verify(self.task4)

    def test_profile_assertion_tamper_detected_after_reexport(self):
        resolution.run(self.task4)
        _,reg=resolution.input_registration(self.task4,verify_input=False)
        with closing(resolution.open_db(self.task4,reg)) as db:
            p=json.loads(db.execute("SELECT profile FROM products WHERE asin=?",(fixtures.ASIN,)).fetchone()[0])
            p["values"]["directors"]=["invented director"]
            with db:db.execute("UPDATE products SET profile=?,profile_hash=? WHERE asin=?",(resolution.json_text(p),resolution.digest_json(p),fixtures.ASIN))
            resolution.export(db,self.task4,resolution.load_rules(self.task4["rules_file"]),reg)
        with self.assertRaisesRegex(ValueError,"raw source"):verify(self.task4)

    def test_history_tamper_detected_after_reexport(self):
        resolution.run(self.task4)
        _,reg=resolution.input_registration(self.task4,verify_input=False)
        with closing(resolution.open_db(self.task4,reg)) as db:
            with db:db.execute("UPDATE mapping_history SET event='{}'")
            resolution.export(db,self.task4,resolution.load_rules(self.task4["rules_file"]),reg)
        with self.assertRaisesRegex(ValueError,"history"):verify(self.task4)

    def test_many_to_many_schema_supports_compilation_without_review_copies(self):
        resolution.run(self.task4)
        _,reg=resolution.input_registration(self.task4,verify_input=False)
        with closing(resolution.open_db(self.task4,reg)) as db:
            with db:
                row=db.execute("SELECT * FROM works").fetchone()
                db.execute("INSERT INTO works VALUES (?,?,?,?,?,?,?)",("mw_synthetic_other","synthetic:other","provisional_singleton",None,None,row["rule_version"],row["run_id"]))
                db.execute("INSERT INTO product_movie VALUES (?,?,'contains','{}',?)",(fixtures.ASIN,"mw_synthetic_other",row["run_id"]))
            self.assertEqual(db.execute("SELECT SUM(review_count) FROM products").fetchone()[0],3)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM product_movie WHERE asin=?",(fixtures.ASIN,)).fetchone()[0],2)

    def test_readonly_inspection_queries_execute(self):
        resolution.run(self.task4)
        with closing(sqlite3.connect((Path(self.task4["work_dir"])/"resolution.sqlite3").as_uri()+"?mode=ro",uri=True)) as db:
            db.executescript((resolution.ROOT/"sql/task4_inspection.sql").read_text(encoding="utf-8"))


if __name__=="__main__":unittest.main()
