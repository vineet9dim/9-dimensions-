import pandas as pd
import re
from difflib import SequenceMatcher
from collections import defaultdict


class AisleIDMatcher:
    def __init__(self, reference_file):
       
        if reference_file.endswith(".xlsx"):
            self.reference_df = pd.read_excel(reference_file)
        else:
            self.reference_df = pd.read_csv(reference_file)

        self.patterns = self._prepare_patterns()
        self.keyword_index = self._create_keyword_index()

    def _clean_text(self, value):
        if pd.isna(value):
            return ""

        text = str(value).lower()
        text = re.sub(r"\(\d+\)", "", text)           # remove (123)
        text = re.sub(r"[^a-z0-9\s]", " ", text)      # remove symbols
        text = re.sub(r"\s+", " ", text).strip()      # normalize spaces
        return text

    def _get_keywords(self, value):
        text = self._clean_text(value)
        return {word for word in text.split() if len(word) > 2}

    def _prepare_patterns(self):
        patterns = []

        for _, row in self.reference_df.iterrows():
            path_parts = []

            for col in ["BC1", "BC2", "BC3", "BC4", "BC5"]:
                if col in row and pd.notna(row[col]):
                    path_parts.append(str(row[col]))

            if not path_parts or "aisle_id" not in row:
                continue

            full_path = " > ".join(path_parts)

            patterns.append({
                "aisle_id": row["aisle_id"],
                "path": path_parts,
                "full_path": full_path,
                "normalized": self._clean_text(full_path),
                "keywords": self._get_keywords(full_path)
            })

        return patterns

    def _create_keyword_index(self):
        index = defaultdict(list)

        for i, pattern in enumerate(self.patterns):
            for word in pattern["keywords"]:
                index[word].append(i)

        return index
    
    def _find_candidates(self, aisle_keywords):
        matched = set()

        for word in aisle_keywords:
            if word in self.keyword_index:
                matched.update(self.keyword_index[word])

        return list(matched) if matched else list(range(len(self.patterns)))

    def _similarity_score(self, aisle_kw, pattern_kw, aisle_txt, pattern_txt):
        if not aisle_kw or not pattern_kw:
            return 0

        common = aisle_kw.intersection(pattern_kw)
        keyword_ratio = len(common) / max(len(aisle_kw), len(pattern_kw))

        if keyword_ratio <= 0.1:
            return keyword_ratio

        text_ratio = SequenceMatcher(None, aisle_txt, pattern_txt).ratio()
        return (0.7 * keyword_ratio) + (0.3 * text_ratio)

    def find_best_match(self, aisle_text):
        aisle_clean = self._clean_text(aisle_text)
        aisle_keywords = self._get_keywords(aisle_text)

        candidates = self._find_candidates(aisle_keywords)

        best_match = None
        best_score = 0

        for idx in candidates:
            pattern = self.patterns[idx]

            score = self._similarity_score(
                aisle_keywords,
                pattern["keywords"],
                aisle_clean,
                pattern["normalized"]
            )

            if score > best_score:
                best_score = score
                best_match = pattern

                if score > 0.9:
                    break

        if best_score > 0.4:
            confidence = "High"
        elif best_score > 0.2:
            confidence = "Medium"
        else:
            confidence = "Low"

        return {
            "aisle_id": best_match["aisle_id"] if best_match else None,
            "confidence": confidence,
            "score": round(best_score, 3),
            "matched_pattern": best_match["full_path"] if best_match else None
        }
    
    def process_target_file(self, target_file, output_file):
        if target_file.endswith(".xlsx"):
            df = pd.read_excel(target_file)
        else:
            df = pd.read_csv(target_file)

        if "aisle" not in df.columns:
            raise ValueError("Target file must contain an 'aisle' column")

        results = []
        total = len(df)

        print(f"Processing {total} rows...")

        for i, row in df.iterrows():
            if i % 500 == 0:
                print(f"{i}/{total} rows done")

            results.append(self.find_best_match(row["aisle"]))

        df["aisle_id"] = [r["aisle_id"] for r in results]
        df["confidence"] = [r["confidence"] for r in results]
        df["match_score"] = [r["score"] for r in results]
        df["matched_pattern"] = [r["matched_pattern"] for r in results]

        df.to_excel(output_file, index=False)
        print("Output saved:", output_file)

        print("\nSummary:")
        print(df["confidence"].value_counts())

        return df


# -----------------------------
# Run script
# -----------------------------
if __name__ == "__main__":
    mapping_file = r"C:\Users\DELL\Desktop\aisle matching\aisle_manual.xlsx"
    products_file = r"C:\Users\DELL\Desktop\aisle matching\product_aisle_with_product_name(19-01-2026).xlsx"
    output_file = r"C:\Users\DELL\Desktop\aisle matching\output_with_aisle_ids1.xlsx"

    print("Loading reference data...")
    matcher = AisleIDMatcher(mapping_file)

    print(f"Patterns loaded: {len(matcher.patterns)}")
    print(f"Keywords indexed: {len(matcher.keyword_index)}")

    result = matcher.process_target_file(products_file, output_file)

    print("\nSample rows:")
    print(result[["aisle", "aisle_id", "confidence", "match_score"]].head())
