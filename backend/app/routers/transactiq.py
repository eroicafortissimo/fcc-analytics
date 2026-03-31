from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import io
import re
import os
import uuid
import json
from collections import Counter

import anthropic

from app.services.nationality_chain import detect_script_region, detect_phonetic_nationality

router = APIRouter()

# In-memory store: analysis_id → {rows, result, bene_col, ord_col}
_analyses: dict = {}

_ORG_KEYWORDS = {
    'ltd', 'limited', 'corp', 'corporation', 'inc', 'llc', 'llp', 'plc',
    'co', 'company', 'bank', 'group', 'holdings', 'holding', 'fund',
    'trust', 'association', 'foundation', 'organization', 'organisation',
    'enterprises', 'enterprise', 'international', 'trading', 'industries',
    'industry', 'services', 'solutions', 'technologies', 'technology',
    'investment', 'investments', 'capital', 'financial', 'finance',
    'gmbh', 'ag', 'sa', 'srl', 'bv', 'nv', 'pty', 'pte', 'jsc', 'ojsc',
}

_BENE_PRIORITIES = [
    'beneficiary_name', 'beneficiary', 'bene_name', 'bene', 'creditor',
    'counterparty', 'customer_name', 'customer', 'party_name',
]

_ORD_PRIORITIES = [
    'ordering_name', 'ordering', 'by_order_name', 'by_order', 'orderer',
    'remitter', 'debtor', 'originator', 'sender',
]

_GENERIC_NAME_PRIORITIES = [
    'name', 'full_name', 'fullname', 'entity_name', 'person', 'subject',
]


def _detect_org(name: str) -> str:
    tokens = re.split(r'[\s,\.]+', name.lower())
    for t in tokens:
        if t.strip('()[]') in _ORG_KEYWORDS:
            return 'entity'
    return 'individual'


def _infer_culture(name: str) -> str:
    script = detect_script_region(name)
    if script:
        return script[0]
    phonetic = detect_phonetic_nationality(name)
    if phonetic:
        return phonetic[0]
    return 'Western / Other'


def _find_col(lower_map: dict, priorities: list) -> Optional[str]:
    for p in priorities:
        for lc, orig in lower_map.items():
            if lc == p or p in lc:
                return orig
    return None


def _auto_detect_cols(columns: list) -> dict:
    lower_map = {c.lower().replace(' ', '_'): c for c in columns}
    bene = _find_col(lower_map, _BENE_PRIORITIES)
    ord_ = _find_col(lower_map, _ORD_PRIORITIES)
    if bene is None and ord_ is None:
        generic = _find_col(lower_map, _GENERIC_NAME_PRIORITIES)
        bene = generic or (columns[0] if columns else None)
    return {'bene': bene, 'ord': ord_}


def _parse_file(content: bytes, filename: str):
    import pandas as pd
    fn = (filename or '').lower()
    if fn.endswith(('.xlsx', '.xls')):
        return pd.read_excel(io.BytesIO(content))
    return pd.read_csv(io.BytesIO(content))


def _analyze_names(df, name_col: str, country_col: Optional[str]):
    """
    Compute all distributions for a single name column.
    Returns (result_dict, culture_series) where culture_series has the same index as the filtered names.
    """
    names = df[name_col].fillna('').astype(str).str.strip()
    names = names[names != '']
    total = len(names)
    if total == 0:
        return None, None

    token_counts  = names.apply(lambda n: len(n.split()))
    char_counts   = names.apply(len)
    culture_series = names.apply(_infer_culture)

    # Culture distribution
    culture_counts = Counter(culture_series.tolist())
    culture_dist = [
        {'culture': k, 'count': v}
        for k, v in sorted(culture_counts.items(), key=lambda x: -x[1])
    ]

    # Per-culture detail (avg tokens/chars)
    culture_detail = []
    for cult in culture_counts:
        mask = culture_series == cult
        tc = token_counts[mask]
        cc = char_counts[mask]
        culture_detail.append({
            'culture': cult,
            'count':      int(culture_counts[cult]),
            'avg_tokens': round(float(tc.mean()), 2),
            'avg_chars':  round(float(cc.mean()), 1),
        })
    culture_detail.sort(key=lambda x: -x['count'])

    # Token count distribution
    tc_raw = Counter(token_counts.tolist())
    token_dist = []
    for i in range(1, 8):
        if i == 7:
            token_dist.append({'tokens': '7+', 'count': sum(v for k, v in tc_raw.items() if k >= 7)})
        else:
            token_dist.append({'tokens': str(i), 'count': tc_raw.get(i, 0)})

    # Name length distribution
    length_buckets = [
        (1,  10,  '1–10'),
        (11, 20,  '11–20'),
        (21, 30,  '21–30'),
        (31, 40,  '31–40'),
        (41, 50,  '41–50'),
        (51, 9999, '51+'),
    ]
    length_dist = [
        {'bucket': label, 'count': int(((char_counts >= lo) & (char_counts <= hi)).sum())}
        for lo, hi, label in length_buckets
    ]

    # Country distribution
    has_country = bool(country_col and country_col in df.columns)
    country_dist = []
    if has_country:
        ctry = df.loc[names.index, country_col].fillna('Unknown').astype(str).str.strip()
        ctry = ctry.replace('', 'Unknown')
        cc_ctr = Counter(ctry.tolist())
        country_dist = [
            {'country': k, 'count': v}
            for k, v in sorted(cc_ctr.items(), key=lambda x: -x[1])
            if k and k.lower() != 'nan'
        ][:30]

    result = {
        'col':            name_col,
        'total':          total,
        'token_dist':     token_dist,
        'length_dist':    length_dist,
        'culture_dist':   culture_dist,
        'culture_detail': culture_detail,
        'country_dist':   country_dist,
        'has_country':    has_country,
        'stats': {
            'avg_tokens': round(float(token_counts.mean()), 2),
            'avg_chars':  round(float(char_counts.mean()), 1),
            'max_tokens': int(token_counts.max()),
            'max_chars':  int(char_counts.max()),
        },
    }
    return result, culture_series


def _build_ai_context(analysis_id: str) -> str:
    """Build a text context string for the AI chat from stored analysis."""
    store = _analyses.get(analysis_id)
    if not store:
        return ''
    result = store['result']
    bene   = result['bene']
    ord_   = result.get('ord')

    lines = ['## Transaction Analytics Context\n']

    lines.append(f"**File**: {store.get('filename', 'unknown')}")
    lines.append(f"**Total rows analyzed**: {store.get('total_rows', 'unknown')}\n")

    lines.append(f"**Beneficiary name column**: {bene['col']}")
    if ord_:
        lines.append(f"**Ordering name column**: {ord_['col']}")

    # Combined stats
    b_total = bene['total']
    o_total = ord_['total'] if ord_ else 0
    total   = b_total + o_total
    lines.append(f"\n**Total names**: {total} (bene: {b_total}" + (f", ord: {o_total})" if ord_ else ")"))

    # Per-column summary
    for section_name, sec in [('Beneficiary', bene), ('Ordering', ord_)]:
        if sec is None:
            continue
        s = sec['stats']
        lines.append(f"\n### {section_name} name stats")
        lines.append(f"- Count: {sec['total']}")
        lines.append(f"- Avg tokens: {s['avg_tokens']}")
        lines.append(f"- Avg chars: {s['avg_chars']}")
        lines.append(f"- Max tokens: {s['max_tokens']}, max chars: {s['max_chars']}")

        lines.append(f"\n**{section_name} culture/region breakdown**:")
        for d in sec.get('culture_detail', sec.get('culture_dist', [])):
            pct = round(d['count'] / sec['total'] * 100, 1) if sec['total'] else 0
            extra = ''
            if 'avg_tokens' in d:
                extra = f", avg_tokens={d['avg_tokens']}, avg_chars={d['avg_chars']}"
            lines.append(f"  - {d['culture']}: {d['count']} ({pct}%){extra}")

        if sec.get('has_country') and sec.get('country_dist'):
            lines.append(f"\n**{section_name} country breakdown (top 15)**:")
            for d in sec['country_dist'][:15]:
                lines.append(f"  - {d['country']}: {d['count']}")

        lines.append(f"\n**{section_name} token distribution**:")
        for d in sec.get('token_dist', []):
            lines.append(f"  - {d['tokens']} token(s): {d['count']}")

        lines.append(f"\n**{section_name} length distribution**:")
        for d in sec.get('length_dist', []):
            lines.append(f"  - {d['bucket']} chars: {d['count']}")

    # Entity type
    et_dist = result.get('entity_type_dist', [])
    if et_dist:
        inferred = result.get('entity_type_inferred', False)
        lines.append(f"\n**Entity type distribution** ({'inferred' if inferred else 'from column'}):")
        for d in et_dist:
            pct = round(d['count'] / b_total * 100, 1) if b_total else 0
            lines.append(f"  - {d['type']}: {d['count']} ({pct}%)")

    return '\n'.join(lines)


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post('/preview')
async def preview_file(file: UploadFile = File(...)):
    """Upload a CSV/Excel file and return detected columns + sample rows."""
    content = await file.read()
    try:
        df = _parse_file(content, file.filename or '')
    except Exception as e:
        raise HTTPException(status_code=422, detail=f'Could not parse file: {e}')

    columns  = list(df.columns.astype(str))
    sample   = df.head(5).fillna('').astype(str).to_dict(orient='records')
    suggested = _auto_detect_cols(columns)
    return {
        'columns':            columns,
        'row_count':          len(df),
        'sample':             sample,
        'suggested_bene_col': suggested['bene'],
        'suggested_ord_col':  suggested['ord'],
    }


@router.post('/analyze')
async def analyze_file(
    file:             UploadFile       = File(...),
    bene_name_col:    str              = Form(...),
    ord_name_col:     Optional[str]    = Form(default=None),
    bene_country_col: Optional[str]    = Form(default=None),
    ord_country_col:  Optional[str]    = Form(default=None),
    entity_type_col:  Optional[str]    = Form(default=None),
):
    content = await file.read()
    try:
        df = _parse_file(content, file.filename or '')
    except Exception as e:
        raise HTTPException(status_code=422, detail=f'Could not parse file: {e}')

    df.columns = df.columns.astype(str)

    if bene_name_col not in df.columns:
        raise HTTPException(status_code=422, detail=f"Column '{bene_name_col}' not found")

    bene, bene_culture = _analyze_names(df, bene_name_col, bene_country_col or None)
    if bene is None:
        raise HTTPException(status_code=422, detail='No non-empty names in the beneficiary column')

    ord_result = None
    ord_culture = None
    if ord_name_col and ord_name_col in df.columns:
        ord_result, ord_culture = _analyze_names(df, ord_name_col, ord_country_col or None)

    # Entity type
    bene_names = df[bene_name_col].fillna('').astype(str).str.strip()
    bene_names = bene_names[bene_names != '']

    has_entity_type = bool(entity_type_col and entity_type_col in df.columns)
    entity_type_inferred = not has_entity_type
    if has_entity_type:
        ets = df.loc[bene_names.index, entity_type_col].fillna('Unknown').astype(str).str.strip()
        ets = ets.replace('', 'Unknown')
        etc = Counter(ets.tolist())
        entity_type_dist = [
            {'type': k, 'count': v}
            for k, v in sorted(etc.items(), key=lambda x: -x[1])
            if k and k.lower() != 'nan'
        ]
    else:
        inferred = bene_names.apply(_detect_org)
        etc = Counter(inferred.tolist())
        entity_type_dist = [
            {'type': k, 'count': v}
            for k, v in sorted(etc.items(), key=lambda x: -x[1])
        ]

    # Build rows list: original df columns + culture column(s)
    df_str = df.fillna('').astype(str)
    if bene_culture is not None:
        df_str.loc[bene_culture.index, '_bene_culture'] = bene_culture.values
        df_str['_bene_culture'] = df_str.get('_bene_culture', '')
    if ord_culture is not None:
        df_str.loc[ord_culture.index, '_ord_culture'] = ord_culture.values
        df_str['_ord_culture'] = df_str.get('_ord_culture', '')
    rows = df_str.to_dict(orient='records')

    analysis_id = str(uuid.uuid4())
    result = {
        'bene':                bene,
        'ord':                 ord_result,
        'entity_type_dist':    entity_type_dist,
        'entity_type_inferred': entity_type_inferred,
    }
    _analyses[analysis_id] = {
        'rows':       rows,
        'result':     result,
        'filename':   file.filename or 'file',
        'total_rows': len(df),
        'columns':    list(df_str.columns),
        'bene_col':   bene_name_col,
        'ord_col':    ord_name_col,
    }

    return {'analysis_id': analysis_id, **result}


@router.get('/rows/{analysis_id}')
async def get_rows(
    analysis_id: str,
    page:      int = 1,
    page_size: int = 25,
):
    store = _analyses.get(analysis_id)
    if not store:
        raise HTTPException(404, 'Analysis not found')
    rows  = store['rows']
    total = len(rows)
    start = (page - 1) * page_size
    return {
        'columns':  store['columns'],
        'bene_col': store['bene_col'],
        'ord_col':  store['ord_col'],
        'total':    total,
        'page':     page,
        'page_size': page_size,
        'rows':     rows[start: start + page_size],
    }


class ChatMessage(BaseModel):
    message: str
    history: List[dict] = []


@router.post('/chat/{analysis_id}')
async def chat(analysis_id: str, body: ChatMessage):
    store = _analyses.get(analysis_id)
    if not store:
        raise HTTPException(404, 'Analysis not found')

    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        raise HTTPException(500, 'ANTHROPIC_API_KEY not configured')

    context = _build_ai_context(analysis_id)
    system_prompt = (
        'You are an expert analytics assistant for transaction name data. '
        'Answer questions concisely and accurately using only the data context provided. '
        'When giving numbers, be precise. If something is not in the context, say so.\n\n'
        + context
    )

    messages = []
    for h in body.history[-10:]:  # last 10 exchanges
        messages.append({'role': h['role'], 'content': h['content']})
    messages.append({'role': 'user', 'content': body.message})

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=512,
        system=system_prompt,
        messages=messages,
    )

    return {'reply': response.content[0].text}
