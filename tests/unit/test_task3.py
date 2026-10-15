"""Synthetic Task3 examples: NOT real facts or independent accuracy labels."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/transform"))
import classify_products as classifier
import test_task2_metadata as metadata_fixtures
import acquire_product_metadata as metadata
from verify_task3 import verify


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.rules = classifier.load_rules(classifier.ROOT / "config/task3.rules.json")

    def item(self, title="Synthetic title", category=None, description=None, **kwargs):
        return dict(asin="B000000001", title=title, category=category or ["Movies & TV"], description=description or [], **kwargs)

    def label(self, **kwargs):
        return classifier.classify_record(self.item(**kwargs), self.rules)["label"]

    def test_format_only_is_not_movie(self):
        for title in ("Mystery DVD", "Drama Blu-ray", "Story VHS"):
            self.assertEqual(self.label(title=title), "awaiting_confirmation")

    def test_generic_movies_tv_and_genre_are_not_movie_evidence(self):
        self.assertEqual(self.label(category=["Movies & TV", "Drama"]), "awaiting_confirmation")

    def test_positive_movie_category(self):
        self.assertEqual(self.label(category=["Movies & TV", "Movies"]), "movie")

    def test_movie_description_needs_corroborating_category(self):
        self.assertEqual(self.label(description=["This film tells a story."]), "awaiting_confirmation")
        self.assertEqual(self.label(category=["Drama"], description=["This film tells a story."]), "movie")

    def test_empty_title_is_unresolved(self):
        self.assertEqual(self.label(title="", category=["Movies"]), "awaiting_confirmation")

    def test_movie_category_does_not_override_tv(self):
        self.assertEqual(self.label(title="Synthetic Complete First Season DVD", category=["Movies", "Boxed Sets"], description=["All episodes of this television series."]), "television_series")

    def test_season_hint_without_corroboration_is_unresolved(self):
        self.assertEqual(self.label(title="Season One", category=["Movies"]), "awaiting_confirmation")

    def test_television_category(self):
        self.assertEqual(self.label(category=["Television Series"]), "television_series")

    def test_boxed_set_alone_is_not_compilation(self):
        self.assertEqual(self.label(title="Synthetic Collector Boxed Set", category=["Boxed Sets"]), "awaiting_confirmation")

    def test_single_movie_multidisc_not_compilation(self):
        self.assertEqual(self.label(title="Synthetic 2-disc DVD", category=["Movies", "Boxed Sets"]), "movie")

    def test_compilation_requires_multiple_films_and_context(self):
        self.assertEqual(self.label(title="Synthetic 3-Movie Collection", category=["Boxed Sets"]), "compilation")
        self.assertEqual(self.label(title="Synthetic Double Feature", category=["Movies"]), "compilation")
        self.assertEqual(self.label(title="Synthetic 3 Film Collection"), "awaiting_confirmation")

    def test_concert_not_all_music(self):
        self.assertEqual(self.label(title="Synthetic Live at Theatre", category=["Music Videos & Concerts"]), "concert")
        self.assertEqual(self.label(category=["Music Videos & Concerts"]), "awaiting_confirmation")

    def test_music_video_other_content(self):
        self.assertEqual(self.label(title="Synthetic Music Videos", category=["Music Videos & Concerts"]), "other_content")

    def test_incidental_music_video_mention_is_not_main_content(self):
        self.assertEqual(self.label(category=["Music Videos & Concerts"], description=["The band pre-dates music videos by decades."]), "awaiting_confirmation")

    def test_bonus_video_does_not_identify_main_content(self):
        self.assertEqual(self.label(category=["Music Videos & Concerts"], description=["A pet entertainment DVD. Extras include a music video."]), "awaiting_confirmation")

    def test_live_from_concert_not_reclassified_by_bonus_video(self):
        self.assertEqual(self.label(title="Synthetic Live From Japan", category=["Music Videos & Concerts"], description=["Also features music videos."]), "concert")

    def test_fitness_and_instruction_non_movie(self):
        self.assertEqual(self.label(title="Synthetic Aerobics Workout DVD", category=["Movies", "Exercise & Fitness"]), "non_movie")
        self.assertEqual(self.label(title="Synthetic Guitar Tutorial", category=["Musicals & Performing Arts"]), "non_movie")

    def test_sports_category_alone_does_not_exclude_movie(self):
        self.assertEqual(self.label(category=["Sports"]), "awaiting_confirmation")

    def test_conflicting_specific_types_unresolved(self):
        self.assertEqual(self.label(title="Synthetic Season 1 Workout", category=["Television Series", "Exercise & Fitness"]), "awaiting_confirmation")

    def test_conflicting_inner_asin_blocks_classification(self):
        item = self.item(category=["Movies"], details={"ASIN: ": "B000000002"})
        self.assertEqual(classifier.classify_record(item, self.rules)["reason"], "inner_asin_conflict")

    def test_fields_preserve_raw_and_missing_semantics(self):
        item = self.item(title=" Synthetic &amp; Title [DVD] (1999)", category=["Drama"], brand="Not a director", date="January 1, 2000", details={"Language: ": "English", "Subtitles: ": "French"})
        fields = classifier.extract_fields(item, self.rules)
        self.assertEqual(fields["title"]["raw"], item["title"])
        self.assertEqual(fields["medium"]["normalized"], ["dvd"])
        self.assertEqual(fields["language"]["field_locators"], ["details.Language: "])
        self.assertEqual(fields["genres"]["normalized"], ["drama"])
        for key in ("directors", "starring", "cast", "release_date", "release_year", "product_release_date"):
            self.assertIsNone(fields[key]["normalized"])
            self.assertTrue(fields[key]["missing_reason"])

    def test_duplicate_candidates_need_consensus_and_titles(self):
        def candidate(item):
            return dict(classifier.classify_record(item, self.rules), fields=classifier.extract_fields(item, self.rules))
        a = candidate(self.item(category=["Movies"]))
        b = candidate(self.item(category=["Movies"], title="Different"))
        self.assertEqual(classifier.consensus([a, a])[0], "movie")
        self.assertEqual(classifier.consensus([a, b])[1], "conflicting_candidate_titles")
        unknown = candidate(self.item())
        self.assertEqual(classifier.consensus([a, unknown])[0], "awaiting_confirmation")
        self.assertEqual(classifier.consensus([])[0], "awaiting_confirmation")

    def test_unsupported_policy_change_rejected(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            rules = deepcopy(self.rules)
            rules["policy"]["dvd_or_bluray_title_is_movie_evidence"] = True
            path = Path(folder) / "rules.json"
            path.write_text(json.dumps(rules))
            with self.assertRaises(ValueError):
                classifier.load_rules(path)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        metadata_fixtures.MetadataTests.setUp(self)
        self.lines = [json.dumps({"asin": metadata_fixtures.ASIN, "title": "Synthetic title DVD", "category": ["Movies"]}).encode()+b"\n"]
        import gzip
        self.archive.write_bytes(gzip.compress(b"".join(self.lines), mtime=0))
        self.registration["archive_sha256"] = classifier.hash_file(self.archive)
        metadata.import_archive(self.metadata_db, self.registration, self.spec, self.fingerprint)
        metadata.export_metadata(self.metadata_db, self.config)
        from acquire_products import export, save_json
        export(self.db, self.config)
        task2_file = self.root / "task2.json"
        save_json(task2_file, self.config)
        self.task3 = dict(task2_config=str(task2_file), rules_file=str(classifier.ROOT / "config/task3.rules.json"),
            work_dir=str(self.root / "task3"), report_dir=str(self.root / "reports3"), batch_size=1)

    def run_task3(self, **kwargs):
        return classifier.run(self.task3, **kwargs)

    def test_full_scope_reviews_and_missing_metadata_preserved(self):
        status, stats = self.run_task3()
        self.assertEqual(status, "complete")
        self.assertEqual(stats["classification_counts"]["movie"], 1)
        self.assertEqual(stats["classification_counts"]["awaiting_confirmation"], 1)
        self.assertEqual(stats["review_references_retained"], 3)
        self.assertEqual(stats["field_coverage"]["all_product_denominator"], 2)
        self.assertEqual(stats["field_coverage"]["identified_movie_product_denominator"], 1)
        self.assertIsNone(stats["field_coverage"]["movie_work_coverage"])
        self.assertEqual(verify(self.task3)["products"], 2)

    def test_same_inputs_idempotent_outputs_and_zero_inserted(self):
        _, first = self.run_task3()
        _, second = self.run_task3()
        self.assertEqual(first["outputs"], second["outputs"])
        last = json.loads((Path(self.task3["report_dir"]) / "last_execution.json").read_text())
        self.assertEqual((last["inserted_products"], last["inserted_candidates"]), (0, 0))

    def test_interrupt_resume_without_skip_or_duplicate(self):
        status, partial = self.run_task3(stop_after_batches=1)
        self.assertEqual(status, "interrupted")
        self.assertEqual(partial["products"], 1)
        with self.assertRaises(ValueError):
            verify(self.task3)
        status, final = self.run_task3()
        self.assertEqual((status, final["products"], final["classification_candidate_records"]), ("complete", 2, 1))
        self.assertEqual(verify(self.task3)["review_references_retained"], 3)

    def test_rule_change_requires_new_versioned_output(self):
        self.run_task3()
        rules = json.loads(Path(self.task3["rules_file"]).read_text())
        rules["version"] = "synthetic-new-version"
        path = self.root / "rules.json"
        path.write_text(json.dumps(rules))
        changed = dict(self.task3, rules_file=str(path))
        with self.assertRaises(ValueError):
            classifier.run(changed)

    def test_tampered_evidence_detected_even_after_reexport(self):
        self.run_task3()
        _, registration = classifier.input_registration(self.task3)
        rules = classifier.load_rules(self.task3["rules_file"])
        with closing(classifier.open_db(self.task3, registration)) as db:
            with db:
                db.execute("UPDATE candidates SET evidence='[]'")
            classifier.export(db, self.task3, rules, registration)
        with self.assertRaisesRegex(ValueError, "replay"):
            verify(self.task3)

    def test_unsupported_real_html_is_not_silently_ignored(self):
        from acquire_products import export
        with self.db:
            self.db.execute("UPDATE products SET status='success' WHERE asin=?", (metadata_fixtures.ASIN,))
        export(self.db, self.config)
        with self.assertRaisesRegex(ValueError, "HTML adapter"):
            classifier.input_registration(self.task3, verify_input=False)

    def test_report_cannot_mix_third_party_with_live_success(self):
        self.run_task3()
        path = Path(self.task3["report_dir"]) / "full_statistics.json"
        stats = json.loads(path.read_text())
        stats["acquisition"]["live_success"] = 1
        from acquire_products import save_json
        save_json(path, stats)
        with self.assertRaisesRegex(ValueError, "measures mixed"):
            verify(self.task3)


if __name__ == "__main__":
    unittest.main()
