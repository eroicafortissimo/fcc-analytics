"""
List Reconciliation Service

Matches public watchlist entities against a private list using 4 tiers:
  1. Exact    — normalized string equality
  2. Expanded — sorted tokens after title-word removal (handles rearrangements)
  3. Fuzzy    — character trigram similarity on sorted-token string (handles spelling variants)
  4. AI       — Claude Haiku for remaining near-miss pairs
"""

import re
import unicodedata
import json
import asyncio
from typing import List, Dict, Tuple, Optional, Callable, Awaitable
import pandas as pd
import io

import anthropic

# Words dropped before token-set comparison
TITLE_WORDS = {
    'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'professor',
    'general', 'gen', 'col', 'colonel', 'major', 'capt', 'captain',
    'admiral', 'commander', 'brigadier', 'lt', 'lieutenant',
    'sheikh', 'sheik', 'shaikh', 'sir', 'lord', 'dame',
    'baron', 'prince', 'princess', 'king', 'queen',
    'minister', 'president', 'the', 'and', 'also',
    'von', 'van', 'de', 'del', 'della', 'la', 'le',
    'bin', 'bint', 'abu', 'um', 'al', 'el',
    'aka', 'known',
}

AI_MODEL = "claude-haiku-4-5-20251001"
AI_BATCH_SIZE = 20
AI_JACCARD_THRESHOLD = 0.15        # lowered — trigrams supplement token matching
AI_TRIGRAM_THRESHOLD = 0.15        # trigram sim floor for sending to AI
AI_MAX_PAIRS = 400                 # hard cap: at most 400 pairs sent to AI
FUZZY_TRIGRAM_THRESHOLD = 0.45     # auto-match without AI


# ---------------------------------------------------------------------------
# Name normalisation helpers
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """ASCII-fold, lowercase, collapse punctuation to spaces."""
    if not name:
        return ''
    name = unicodedata.normalize('NFKD', str(name))
    name = name.encode('ascii', 'ignore').decode()
    name = name.lower()
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


def _significant_tokens(name: str) -> frozenset:
    """Tokens after normalisation, dropping title words and single chars."""
    tokens = _normalize(name).split()
    return frozenset(t for t in tokens if t not in TITLE_WORDS and len(t) > 1)


def _expanded_key(name: str) -> str:
    """Sorted significant tokens — handles reordering & title dropping."""
    toks = _significant_tokens(name)
    return ' '.join(sorted(toks)) if toks else ''


def _expanded_trigrams(name: str) -> frozenset:
    """
    Character trigrams of the sorted-significant-token string (spaces removed).
    Handles spelling variants like Mohammed/Mohammad, Abdulrahman/Abdul Rahman.
    """
    key = _expanded_key(name).replace(' ', '')
    if not key:
        return frozenset()
    if len(key) < 3:
        return frozenset({key})
    return frozenset(key[i:i+3] for i in range(len(key) - 2))


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


# ---------------------------------------------------------------------------
# Private list parsing
# ---------------------------------------------------------------------------

def parse_private_list(
    file_bytes: bytes,
    filename: str,
) -> Tuple[List[Dict], str, Optional[str]]:
    """
    Parse a CSV or Excel file into entities.

    Returns:
        (entries, detected_name_col, detected_aka_col)
        Each entry: { 'name': str, 'akas': [str] }
    """
    if filename.lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        df = pd.read_csv(io.BytesIO(file_bytes))

    df.columns = [str(c).strip() for c in df.columns]
    lower_cols = {c.lower(): c for c in df.columns}

    # Detect name column
    name_col = None
    for candidate in ['name', 'primary_name', 'entity_name', 'full_name', 'entity', 'party_name']:
        if candidate in lower_cols:
            name_col = lower_cols[candidate]
            break
    if name_col is None:
        for col in df.columns:
            if df[col].dtype == object:
                name_col = col
                break
    if name_col is None:
        raise ValueError("Could not detect a name column in the uploaded file.")

    # Detect AKA column
    aka_col = None
    for candidate in ['aka', 'akas', 'alias', 'aliases', 'alternate_name',
                      'alternate_names', 'also_known_as', 'other_names']:
        if candidate in lower_cols:
            aka_col = lower_cols[candidate]
            break

    entries: List[Dict] = []
    for _, row in df.iterrows():
        raw = row[name_col]
        if pd.isna(raw):
            continue
        name = str(raw).strip()
        if not name or name.lower() == 'nan':
            continue
        akas: List[str] = []
        if aka_col and pd.notna(row.get(aka_col, None)):
            raw_aka = str(row[aka_col])
            akas = [a.strip() for a in re.split(r'[|;]', raw_aka) if a.strip()]
        entries.append({'name': name, 'akas': akas})

    return entries, name_col, aka_col


# ---------------------------------------------------------------------------
# Entity index
# ---------------------------------------------------------------------------

class EntityIndex:
    """Fast lookup index for a list of entities."""

    def __init__(self, entities: List[Dict]):
        self.entities = entities
        self._exact: Dict[str, Dict] = {}          # norm → first entity
        self._expanded: Dict[str, Dict] = {}       # expanded_key → first entity
        self._by_token: Dict[str, List[Dict]] = {} # token → [entity, ...]
        self._by_trigram: Dict[str, List[Dict]] = {}  # trigram → [entity, ...]

        for entity in entities:
            all_names = [entity['name']] + entity.get('akas', [])
            norms: set = set()
            exps: set = set()
            toks: frozenset = frozenset()
            tgs: frozenset = frozenset()

            for n in all_names:
                norm = _normalize(n)
                exp = _expanded_key(n)
                t = _significant_tokens(n)
                tg = _expanded_trigrams(n)

                if norm:
                    norms.add(norm)
                    self._exact.setdefault(norm, entity)
                if exp:
                    exps.add(exp)
                    self._expanded.setdefault(exp, entity)
                for tok in t:
                    self._by_token.setdefault(tok, []).append(entity)
                for trigram in tg:
                    self._by_trigram.setdefault(trigram, []).append(entity)

                toks = toks | t
                tgs = tgs | tg

            entity['_norms'] = norms
            entity['_exps'] = exps
            entity['_toks'] = toks
            entity['_tgs'] = tgs

    def find_exact(self, entity: Dict) -> Optional[Dict]:
        for norm in entity.get('_norms', set()):
            if norm in self._exact:
                return self._exact[norm]
        return None

    def find_expanded(self, entity: Dict) -> Optional[Dict]:
        for exp in entity.get('_exps', set()):
            if exp and exp in self._expanded:
                return self._expanded[exp]
        return None

    def find_fuzzy(self, entity: Dict, threshold: float = FUZZY_TRIGRAM_THRESHOLD) -> Optional[Dict]:
        """Find best trigram-similar entity above threshold (Tier 3 auto-match)."""
        tgs = entity.get('_tgs', frozenset())
        if not tgs:
            return None

        # Collect candidates via shared trigrams
        seen: Dict[int, Tuple[Dict, int]] = {}
        for tg in tgs:
            for cand in self._by_trigram.get(tg, []):
                cid = id(cand)
                if cid == id(entity):
                    continue
                if cid in seen:
                    seen[cid] = (cand, seen[cid][1] + 1)
                else:
                    seen[cid] = (cand, 1)

        best_entity = None
        best_score = threshold

        for cand, _ in seen.values():
            score = _jaccard(tgs, cand.get('_tgs', frozenset()))
            if score >= best_score:
                best_score = score
                best_entity = cand

        return best_entity

    def find_candidates(self, entity: Dict, threshold: float = AI_JACCARD_THRESHOLD) -> List[Tuple[Dict, float]]:
        """
        Return (candidate, score) pairs for AI review, best first.
        Uses both token Jaccard and trigram similarity to find candidates.
        """
        toks = entity.get('_toks', frozenset())
        tgs = entity.get('_tgs', frozenset())

        seen: Dict[int, Dict] = {}  # id → entity

        # Token-based candidates
        for tok in toks:
            for cand in self._by_token.get(tok, []):
                cid = id(cand)
                if cid != id(entity):
                    seen[cid] = cand

        # Trigram-based candidates (catches spelling variants with no token overlap)
        for tg in tgs:
            for cand in self._by_trigram.get(tg, []):
                cid = id(cand)
                if cid != id(entity):
                    seen[cid] = cand

        results = []
        for cand in seen.values():
            tok_j = _jaccard(toks, cand.get('_toks', frozenset()))
            tg_j = _jaccard(tgs, cand.get('_tgs', frozenset()))
            score = max(tok_j, tg_j)
            if score >= threshold:
                results.append((cand, score))

        results.sort(key=lambda x: -x[1])
        return results[:5]


# ---------------------------------------------------------------------------
# AI matching (Claude Haiku)
# ---------------------------------------------------------------------------

async def _ai_match(
    pub_unmatched: List[Dict],
    priv_unmatched_idx: EntityIndex,
    stats: Dict,
    progress_cb: Callable,
) -> None:
    """Mark AI-matched pairs in place."""
    # Build candidate pairs
    pairs: List[Tuple[Dict, Dict, float]] = []
    for pub_e in pub_unmatched:
        candidates = priv_unmatched_idx.find_candidates(pub_e)
        if candidates:
            best_cand, best_j = candidates[0]
            pairs.append((pub_e, best_cand, best_j))

    if not pairs:
        return

    # Sort by score descending, then cap
    pairs.sort(key=lambda x: -x[2])
    if len(pairs) > AI_MAX_PAIRS:
        pairs = pairs[:AI_MAX_PAIRS]

    client = anthropic.AsyncAnthropic()
    total = len(pairs)

    for batch_start in range(0, total, AI_BATCH_SIZE):
        batch = pairs[batch_start: batch_start + AI_BATCH_SIZE]

        lines = []
        for i, (pub, priv, _) in enumerate(batch):
            pub_str = pub['name']
            if pub.get('akas'):
                pub_str += f" (also: {'; '.join(pub['akas'][:2])})"
            priv_str = priv['name']
            if priv.get('akas'):
                priv_str += f" (also: {'; '.join(priv['akas'][:2])})"
            lines.append(f'{i + 1}. Public="{pub_str}" | Private="{priv_str}"')

        prompt = (
            "You are an expert at matching sanctions list names across different spellings, "
            "transliterations, and formats. For each numbered pair decide if both sides refer "
            "to the SAME person or entity. Consider: name rearrangements, dropped/added titles "
            "(General, Sheikh, etc.), transliteration variants, and common nicknames. "
            "Reply ONLY with a JSON array of booleans — one per pair, in order "
            "(true = same entity, false = different).\n\n"
            + "\n".join(lines)
        )

        try:
            resp = await client.messages.create(
                model=AI_MODEL,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            arr_match = re.search(r'\[[\s\S]*?\]', text)
            if arr_match:
                results = json.loads(arr_match.group())
                for i, is_same in enumerate(results):
                    if i < len(batch) and is_same:
                        pub_e, priv_e, _ = batch[i]
                        if not pub_e.get('match_tier'):
                            pub_e['match_tier'] = 'ai'
                            pub_e['matched_to'] = priv_e['name']
                            pub_e['matched_key'] = priv_e.get('key', priv_e['name'])
                        if not priv_e.get('match_tier'):
                            priv_e['match_tier'] = 'ai'
                            priv_e['matched_to'] = pub_e['name']
                        stats['matched_ai'] += 1
        except Exception:
            pass  # AI failures are non-fatal; tiers 1–3 already ran

        done = min(batch_start + AI_BATCH_SIZE, total)
        await progress_cb(50 + int(40 * done / total), f"AI matching: {done}/{total} pairs...")


# ---------------------------------------------------------------------------
# Main reconciliation entry point
# ---------------------------------------------------------------------------

def _clean(entries: List[Dict]) -> List[Dict]:
    """Remove internal _* fields and make JSON-serialisable."""
    result = []
    for e in entries:
        clean = {}
        for k, v in e.items():
            if k.startswith('_'):
                continue
            if isinstance(v, (set, frozenset)):
                clean[k] = sorted(v)
            else:
                clean[k] = v
        result.append(clean)
    return result


async def run_reconciliation(
    public_entities: List[Dict],
    private_entities: List[Dict],
    use_ai: bool = True,
    progress_cb: Optional[Callable[[int, str], Awaitable[None]]] = None,
) -> Dict:
    """
    Reconcile public vs private entities.

    Returns:
      {
        'public_not_on_private': [...],
        'private_not_on_public': [...],
        'matches': [...],
        'full_public': [...],
        'stats': { total_public, total_private, matched_exact, matched_expanded,
                   matched_fuzzy, matched_ai, unmatched_public, unmatched_private }
      }
    """
    async def _prog(pct: int, msg: str) -> None:
        if progress_cb:
            await progress_cb(pct, msg)

    await _prog(5, "Building indexes...")

    priv_idx = EntityIndex(private_entities)
    pub_idx = EntityIndex(public_entities)

    stats = {
        'total_public': len(public_entities),
        'total_private': len(private_entities),
        'matched_exact': 0,
        'matched_expanded': 0,
        'matched_fuzzy': 0,
        'matched_ai': 0,
        'unmatched_public': 0,
        'unmatched_private': 0,
    }

    await _prog(18, "Tier 1 & 2: exact and expanded matching...")

    pub_unmatched: List[Dict] = []
    for pub_e in public_entities:
        match = priv_idx.find_exact(pub_e)
        if match:
            pub_e['match_tier'] = 'exact'
            pub_e['matched_to'] = match['name']
            pub_e['matched_key'] = match.get('key', match['name'])
            stats['matched_exact'] += 1
        else:
            match = priv_idx.find_expanded(pub_e)
            if match:
                pub_e['match_tier'] = 'expanded'
                pub_e['matched_to'] = match['name']
                pub_e['matched_key'] = match.get('key', match['name'])
                stats['matched_expanded'] += 1
            else:
                pub_unmatched.append(pub_e)

    priv_unmatched: List[Dict] = []
    for priv_e in private_entities:
        match = pub_idx.find_exact(priv_e)
        if match:
            priv_e['match_tier'] = 'exact'
            priv_e['matched_to'] = match['name']
        else:
            match = pub_idx.find_expanded(priv_e)
            if match:
                priv_e['match_tier'] = 'expanded'
                priv_e['matched_to'] = match['name']
            else:
                priv_unmatched.append(priv_e)

    await _prog(38, f"Tiers 1+2 done. {len(pub_unmatched)} public and {len(priv_unmatched)} private still unmatched.")

    # -------------------------------------------------------------------------
    # Tier 3: Fuzzy trigram matching
    # -------------------------------------------------------------------------
    await _prog(40, "Tier 3: fuzzy trigram matching...")

    priv_unmatched_idx_for_fuzzy = EntityIndex(priv_unmatched)
    pub_unmatched_idx_for_fuzzy = EntityIndex(pub_unmatched)

    still_pub_unmatched: List[Dict] = []
    for pub_e in pub_unmatched:
        match = priv_unmatched_idx_for_fuzzy.find_fuzzy(pub_e)
        if match and not match.get('match_tier'):
            pub_e['match_tier'] = 'fuzzy'
            pub_e['matched_to'] = match['name']
            pub_e['matched_key'] = match.get('key', match['name'])
            match['match_tier'] = 'fuzzy'
            match['matched_to'] = pub_e['name']
            stats['matched_fuzzy'] += 1
        else:
            still_pub_unmatched.append(pub_e)

    # Also run priv→pub fuzzy pass to catch any priv entities not covered above
    for priv_e in priv_unmatched:
        if not priv_e.get('match_tier'):
            match = pub_unmatched_idx_for_fuzzy.find_fuzzy(priv_e)
            if match and not match.get('match_tier'):
                priv_e['match_tier'] = 'fuzzy'
                priv_e['matched_to'] = match['name']
                match['match_tier'] = 'fuzzy'
                match['matched_to'] = priv_e['name']
                match['matched_key'] = priv_e.get('key', priv_e['name'])
                stats['matched_fuzzy'] += 1

    still_pub_unmatched = [e for e in pub_unmatched if not e.get('match_tier')]
    still_priv_unmatched = [e for e in priv_unmatched if not e.get('match_tier')]

    await _prog(47, f"Tier 3 done. {len(still_pub_unmatched)} public and {len(still_priv_unmatched)} private still unmatched.")

    if use_ai and still_pub_unmatched and still_priv_unmatched:
        await _prog(48, "Building AI candidate index...")
        priv_unmatched_idx = EntityIndex(still_priv_unmatched)
        await _ai_match(still_pub_unmatched, priv_unmatched_idx, stats, _prog)

    await _prog(93, "Finalising results...")

    final_pub = [e for e in public_entities if not e.get('match_tier')]
    final_priv = [e for e in private_entities if not e.get('match_tier')]

    stats['unmatched_public'] = len(final_pub)
    stats['unmatched_private'] = len(final_priv)

    matched_pub = [e for e in public_entities if e.get('match_tier')]
    stats['matched_total'] = len(matched_pub)

    return {
        'public_not_on_private': _clean(final_pub),
        'private_not_on_public': _clean(final_priv),
        'matches': _clean(matched_pub),
        'full_public': _clean(public_entities),
        'private_list': _clean(private_entities),
        'stats': stats,
    }
