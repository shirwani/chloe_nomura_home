import math
import os
import re
from typing import List, Dict, Any

from openai import OpenAI
from rapidfuzz import fuzz

from database import DBInterface


class InventorySearch:
    """
    Helper class for performing keyword + semantic search over the inventory table.

    - Keyword search: basic token overlap between the query and item text.
    - Semantic search: cosine similarity between OpenAI embeddings for the query
      and each item's text (name + description).

    The OpenAI client will use an API key loaded from one of:
      - the OPENAI_API_KEY environment variable (standard)
      - the OPENAI_API_KEY_PATH environment variable
      - the custom openai.api_key_path environment variable (file containing the key)
    """

    EMBEDDING_MODEL = "text-embedding-3-small"

    def __init__(self, db: DBInterface | None = None, client: OpenAI | None = None):
        # Allow callers to provide a shared DBInterface (e.g., per-request),
        # falling back to creating a new one when not supplied.
        self.db = db or DBInterface()
        self.client = client or self._build_client()

    def _build_client(self) -> OpenAI:
        """
        Build an OpenAI client, honoring a file-based API key if configured.
        """
        # First, allow providing the key directly via environment variable
        direct_key = os.environ.get("OPENAI_API_KEY")
        if direct_key:
            return OpenAI(api_key=direct_key.strip())

        # Next, allow a file path environment variable containing the key
        key_path = (
            os.environ.get("OPENAI_API_KEY_PATH")
            or os.environ.get("openai.api_key_path")
        )
        if key_path and os.path.exists(key_path):
            with open(key_path, "r", encoding="utf-8") as f:
                file_key = f.read().strip()
            if file_key:
                return OpenAI(api_key=file_key)

        # Fall back to default OpenAI configuration (may read other env vars or config)
        return OpenAI()

    @staticmethod
    def _normalize_tokens(text: str) -> set:
        """
        Very simple tokenization + lowercasing for keyword / fuzzy overlap.
        """
        tokens = re.findall(r"\w+", text or "")
        return {t.lower() for t in tokens if t}

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        """
        Compute cosine similarity between two embedding vectors.
        """
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = 0.0
        na = 0.0
        nb = 0.0
        for x, y in zip(a, b):
            dot += x * y
            na += x * x
            nb += y * y
        if na == 0.0 or nb == 0.0:
            return 0.0
        return dot / (math.sqrt(na) * math.sqrt(nb))

    @staticmethod
    def _item_text(item) -> str:
        """
        Build a text representation for an item using its id, name, description,
        and category so that all of these fields can be searched.
        """
        item_id = str(getattr(item, "id", "") or "")
        name = getattr(item, "name", "") or ""
        desc = getattr(item, "description", "") or ""
        category = getattr(item, "category", "") or ""
        return " ".join(part for part in [item_id, name, desc, category] if part).strip()

    @staticmethod
    def _fuzzy_token_score(query_tokens: set, item_tokens: set) -> float:
        """
        Compute a fuzzy token similarity score in [0, 1] that is tolerant of
        minor spelling errors and typos, using RapidFuzz.
        """
        if not query_tokens or not item_tokens:
            return 0.0

        total_best = 0.0
        for q in query_tokens:
            best = 0.0
            for t in item_tokens:
                score = fuzz.ratio(q, t)
                if score > best:
                    best = score
            total_best += best

        # Average best-match ratio across all query tokens and normalize to [0,1]
        return total_best / (100.0 * len(query_tokens))

    def search(self, query: str, top_k: int = 20) -> List[Dict[str, Any]]:
        """
        Perform a combined keyword + semantic search over all inventory items.

        Returns a list of results sorted by combined score, where each result is:
            {
                "item": <inventory row>,
                "semantic_score": float,
                "keyword_score": float,
                "combined_score": float,
            }
        """
        query = (query or "").strip()
        if not query:
            return []

        # Load all items from the inventory table
        items = list(self.db.get_all_data("inventory"))
        if not items:
            return []

        # Build plain-text representations and tokens for keyword scoring
        item_texts: List[str] = [self._item_text(item) for item in items]
        query_tokens = self._normalize_tokens(query)
        item_tokens_list = [self._normalize_tokens(text) for text in item_texts]
        # Also keep category tokens separately so we can weight category matches higher
        category_texts: List[str] = [
            str(getattr(item, "category", "") or "") for item in items
        ]
        category_tokens_list = [
            self._normalize_tokens(text) for text in category_texts
        ]

        # Get embeddings for the query and all item texts in a single API call
        inputs = [query] + item_texts
        response = self.client.embeddings.create(
            model=self.EMBEDDING_MODEL,
            input=inputs,
        )

        vectors = [d.embedding for d in response.data]
        query_vec = vectors[0]
        item_vecs = vectors[1:]

        results: List[Dict[str, Any]] = []
        for item, item_vec, item_tokens, category_tokens in zip(
            items, item_vecs, item_tokens_list, category_tokens_list
        ):
            semantic_score = self._cosine_similarity(query_vec, item_vec)
            exact_keyword_score = (
                float(len(query_tokens & item_tokens)) / float(len(query_tokens))
                if query_tokens
                else 0.0
            )
            fuzzy_keyword_score = self._fuzzy_token_score(query_tokens, item_tokens)
            # Use the stronger of exact token overlap or fuzzy match as the keyword score
            keyword_score = max(exact_keyword_score, fuzzy_keyword_score)

            # Compute a separate category score (exact + fuzzy) so we can weight it more
            exact_category_score = (
                float(len(query_tokens & category_tokens)) / float(len(query_tokens))
                if query_tokens
                else 0.0
            )
            fuzzy_category_score = self._fuzzy_token_score(
                query_tokens, category_tokens
            )
            category_score = max(exact_category_score, fuzzy_category_score)

            # Weight category matches more heavily, but still factor in overall semantics
            # and general keyword matches.
            combined_score = (
                0.3 * semantic_score
                + 0.4 * keyword_score
                + 0.3 * category_score
            )

            # Only keep results that meet a minimum relevance threshold
            if combined_score > 0.3:
                results.append(
                    {
                        "item": item,
                        "semantic_score": semantic_score,
                        "keyword_score": keyword_score,
                        "combined_score": combined_score,
                    }
                )

        # Sort by combined score (highest first) and optionally truncate to top_k
        results.sort(key=lambda r: r["combined_score"], reverse=True)
        if top_k is not None and top_k > 0:
            results = results[:top_k]
        return results


if __name__ == "__main__":
    searcher = InventorySearch()
    try:
        query = "chair with green fabric"
        results = searcher.search(query)
        for r in results:
            item = r["item"]
            print(
                f"{r['combined_score']:.3f} "
                f"(semantic={r['semantic_score']:.3f}, keyword={r['keyword_score']:.3f}) "
                f"- {getattr(item, 'name', '')} [id={getattr(item, 'id', '')}]"
            )
    finally:
        searcher.db.shutdown()
