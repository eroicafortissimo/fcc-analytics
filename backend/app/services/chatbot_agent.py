"""
chatbot_agent.py — LangGraph human-in-the-loop chatbot for new test case types.

Conversation flow:
  1. User describes a new variation type in natural language
  2. Agent (Claude Haiku) extracts a structured type definition + python_lambda
  3. Agent applies lambda to 5 sample names from the watchlist → shows examples
  4. User either confirms ("yes" / "confirm") or gives feedback (→ refine)
  5. On confirm: type saved to custom_test_types DB table and returned to caller
"""
from __future__ import annotations

import ast
import json
import os
import random
import re
import uuid
from typing import Optional

import aiosqlite
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict


# ── LLM ────────────────────────────────────────────────────────────────────────

def _llm():
    return ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        temperature=0,
        max_tokens=1024,
    )


# ── State ───────────────────────────────────────────────────────────────────────

class ChatState(TypedDict):
    session_id: str
    user_message: str
    stage: str               # 'new' | 'proposing' | 'saved'
    proposed_type: Optional[dict]
    examples: list[str]
    iteration: int
    response: str            # Final assistant message to return
    sample_names: list[str]  # Pre-loaded from DB before graph runs


# ── System prompts ──────────────────────────────────────────────────────────────

_EXTRACT_SYSTEM = """\
You are a sanctions screening test case designer helping users create name variation test types.

Your job: parse the user's natural-language description and return a JSON object with these exact fields:

{
  "type_name": "Short descriptive name (3-6 words)",
  "description": "One sentence description of what this variation does",
  "applicable_entity_types": ["individual", "entity"],
  "applicable_min_tokens": 1,
  "applicable_min_name_length": 1,
  "expected_outcome": "Should Hit",
  "variation_logic": "Step-by-step plain-English description of the algorithm",
  "python_lambda": "lambda name, rng: ..."
}

Rules for python_lambda:
- Must be a valid Python lambda expression (not a statement)
- Arguments: name (str), rng (random.Random)
- Return: modified name string, or None if variation cannot apply
- Use only built-in str/list operations — no imports
- If the variation requires multiple tokens, check len(name.split()) first

expected_outcome must be one of: "Must Hit", "Should Hit", "Testing Purposes", "Should Not Hit"

applicable_entity_types must be a subset of: ["individual", "entity", "vessel", "aircraft", "country", "unknown"]

Examples of valid python_lambda values:
- Compress: lambda name, rng: name.replace(' ', '') if ' ' in name else None
- Initial first: lambda name, rng: (name.split()[0][0] + '. ' + ' '.join(name.split()[1:])) if len(name.split()) > 1 else None
- Two-letter prefix per token: lambda name, rng: ' '.join(t[:2].upper() for t in name.split()) if len(name.split()) > 1 else None

Return ONLY valid JSON. No markdown, no explanation outside the JSON object.
"""

_REFINE_SYSTEM = """\
You are refining a sanctions screening test case type definition based on user feedback.

Current proposed type (JSON):
{current_json}

User feedback: {feedback}

Return an updated JSON object with the same fields. Apply the user's feedback precisely.
Return ONLY valid JSON. No markdown, no explanation.
"""


# ── Lambda evaluation ───────────────────────────────────────────────────────────

# Safe builtins available to LLM-generated lambdas (no IO, no eval, no import)
_SAFE_BUILTINS = {
    'len': len, 'str': str, 'int': int, 'list': list, 'tuple': tuple,
    'range': range, 'enumerate': enumerate, 'zip': zip,
    'min': min, 'max': max, 'sum': sum,
    'sorted': sorted, 'reversed': reversed,
    'any': any, 'all': all,
    'chr': chr, 'ord': ord,
    'upper': str.upper, 'lower': str.lower,
    'True': True, 'False': False, 'None': None,
}


def _safe_apply(lambda_str: str, name: str, rng: random.Random) -> Optional[str]:
    """
    Safely compile and evaluate an LLM-generated lambda against a name.
    Parses to AST first to ensure it is a lambda expression.
    """
    try:
        tree = ast.parse(lambda_str.strip(), mode='eval')
        if not isinstance(tree.body, ast.Lambda):
            return None
        code = compile(tree, '<lambda>', 'eval')
        fn = eval(code, {"__builtins__": _SAFE_BUILTINS}, {})  # noqa: S307
        result = fn(name, rng)
        if isinstance(result, str) and result.strip():
            return result.strip()
        return None
    except Exception:
        return None


# ── Graph nodes ─────────────────────────────────────────────────────────────────

def _parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, stripping any markdown fences."""
    text = text.strip()
    # Strip ```json ... ``` or ``` ... ```
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return json.loads(text)


def node_extract_intent(state: ChatState) -> dict:
    """Call Claude to parse user description → structured type dict."""
    llm = _llm()
    messages = [
        SystemMessage(content=_EXTRACT_SYSTEM),
        HumanMessage(content=state['user_message']),
    ]
    response = llm.invoke(messages)
    try:
        proposed = _parse_json_response(response.content)
    except Exception:
        return {
            'proposed_type': None,
            'response': (
                "Sorry, I couldn't parse a clear variation type from your description. "
                "Could you rephrase? For example: \"Create a test where you take the first "
                "letter of each token and join them with dots.\""
            ),
            'stage': 'new',
        }
    return {'proposed_type': proposed, 'stage': 'proposing'}


def node_refine_type(state: ChatState) -> dict:
    """Call Claude to refine the proposed type based on user feedback."""
    llm = _llm()
    current_json = json.dumps(state['proposed_type'], indent=2)
    system = _REFINE_SYSTEM.format(
        current_json=current_json,
        feedback=state['user_message'],
    )
    messages = [SystemMessage(content=system), HumanMessage(content="Refine the type.")]
    response = llm.invoke(messages)
    try:
        proposed = _parse_json_response(response.content)
    except Exception:
        proposed = state['proposed_type']  # Keep old if parse fails
    return {'proposed_type': proposed, 'stage': 'proposing', 'iteration': state['iteration'] + 1}


def node_generate_examples(state: ChatState) -> dict:
    """Apply the proposed lambda to sample names and format an examples block."""
    proposed = state.get('proposed_type')
    if not proposed:
        return {'examples': [], 'response': state.get('response', '')}

    lambda_str = proposed.get('python_lambda', '')
    rng = random.Random(42)
    examples = []
    for name in state.get('sample_names', []):
        if len(examples) >= 5:
            break
        result = _safe_apply(lambda_str, name, rng)
        if result and result != name:
            examples.append(f"  • {name}  →  {result}")

    if not examples:
        examples = ['  (No examples could be generated — check the lambda or entity type constraints)']

    # Format the full proposal reply
    p = proposed
    reply_lines = [
        f"Here's the proposed test case type:\n",
        f"**Name:** {p.get('type_name', '?')}",
        f"**Description:** {p.get('description', '?')}",
        f"**Expected outcome:** {p.get('expected_outcome', 'Should Hit')}",
        f"**Applies to:** {', '.join(p.get('applicable_entity_types', ['all']))}",
        f"**Min tokens:** {p.get('applicable_min_tokens', 1)}, "
        f"**Min length:** {p.get('applicable_min_name_length', 1)}",
        f"\n**Variation logic:** {p.get('variation_logic', '?')}",
        f"\n**Examples from the watchlist:**",
    ] + examples + [
        "\n---",
        "Type **confirm** to save this type, or describe any changes you'd like.",
    ]

    return {'examples': examples, 'response': '\n'.join(reply_lines)}


def node_save_type(state: ChatState) -> dict:
    """Signal that the type should be saved (actual DB write happens in handle_message)."""
    p = state.get('proposed_type', {})
    name = p.get('type_name', 'Custom Type')
    return {
        'stage': 'saved',
        'response': (
            f"**Saved!** The new type **\"{name}\"** has been added to your active types.\n\n"
            "It will now appear in the Variation Types panel. Select it and click **Generate** "
            "to create test cases.\n\n"
            "You can start describing another new type whenever you're ready."
        ),
    }


# ── Router (conditional edge) ───────────────────────────────────────────────────

_CONFIRM_WORDS = {
    'confirm', 'yes', 'save', 'ok', 'okay', 'approved', 'approve',
    'looks good', 'correct', 'perfect', 'great', 'done', 'go ahead', 'proceed',
}

def route_stage(state: ChatState) -> str:
    stage = state.get('stage', 'new')
    msg = state.get('user_message', '').lower().strip()
    if stage == 'proposing':
        if any(w in msg for w in _CONFIRM_WORDS):
            return 'save_type'
        return 'refine_type'
    return 'extract_intent'


# ── Build graph ─────────────────────────────────────────────────────────────────

def _build_graph():
    g = StateGraph(ChatState)
    g.add_node('extract_intent', node_extract_intent)
    g.add_node('refine_type', node_refine_type)
    g.add_node('generate_examples', node_generate_examples)
    g.add_node('save_type', node_save_type)

    g.add_conditional_edges(START, route_stage, {
        'extract_intent': 'extract_intent',
        'refine_type': 'refine_type',
        'save_type': 'save_type',
    })
    g.add_edge('extract_intent', 'generate_examples')
    g.add_edge('refine_type', 'generate_examples')
    g.add_edge('generate_examples', END)
    g.add_edge('save_type', END)

    return g.compile()


_GRAPH = None

def get_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH


# ── Session persistence helpers ─────────────────────────────────────────────────

async def _load_session(session_id: str, db: aiosqlite.Connection) -> dict:
    async with db.execute(
        "SELECT stage, proposed_type, examples, iteration, messages FROM chatbot_sessions WHERE session_id = ?",
        (session_id,),
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return {'stage': 'new', 'proposed_type': None, 'examples': [], 'iteration': 0, 'messages': []}
    return {
        'stage': row[0],
        'proposed_type': json.loads(row[1]) if row[1] else None,
        'examples': json.loads(row[2]) if row[2] else [],
        'iteration': row[3] or 0,
        'messages': json.loads(row[4]) if row[4] else [],
    }


async def _save_session(session_id: str, state: dict, db: aiosqlite.Connection):
    await db.execute(
        """INSERT INTO chatbot_sessions (session_id, stage, proposed_type, examples, iteration, messages, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(session_id) DO UPDATE SET
               stage=excluded.stage,
               proposed_type=excluded.proposed_type,
               examples=excluded.examples,
               iteration=excluded.iteration,
               messages=excluded.messages,
               updated_at=excluded.updated_at""",
        (
            session_id,
            state.get('stage', 'new'),
            json.dumps(state.get('proposed_type')) if state.get('proposed_type') else None,
            json.dumps(state.get('examples', [])),
            state.get('iteration', 0),
            json.dumps(state.get('messages', [])),
        ),
    )
    await db.commit()


async def _save_custom_type(proposed: dict, db: aiosqlite.Connection) -> str:
    """Assign a USER### ID and persist to custom_test_types."""
    async with db.execute("SELECT COUNT(*) FROM custom_test_types") as cur:
        count = (await cur.fetchone())[0]
    type_id = f"USER{count + 1:03d}"
    entity_types = '|'.join(proposed.get('applicable_entity_types', ['individual', 'entity']))
    await db.execute(
        """INSERT OR IGNORE INTO custom_test_types
           (type_id, theme, category, type_name, description,
            applicable_entity_types, applicable_min_tokens, applicable_min_name_length,
            expected_outcome, variation_logic, python_lambda)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            type_id,
            proposed.get('theme', 'Custom'),
            proposed.get('category', 'User-Defined'),
            proposed.get('type_name', 'Custom Type'),
            proposed.get('description', ''),
            entity_types,
            proposed.get('applicable_min_tokens', 1),
            proposed.get('applicable_min_name_length', 1),
            proposed.get('expected_outcome', 'Should Hit'),
            proposed.get('variation_logic', ''),
            proposed.get('python_lambda', ''),
        ),
    )
    await db.commit()
    return type_id


async def _load_sample_names(proposed: dict, db: aiosqlite.Connection, n: int = 10) -> list[str]:
    """Load sample names matching the proposed type's entity type constraints."""
    entity_types = proposed.get('applicable_entity_types', [])
    all_types = {'individual', 'entity', 'vessel', 'aircraft', 'country', 'unknown'}
    conditions = []
    params: list = []
    if entity_types and set(entity_types) != all_types:
        ph = ', '.join('?' for _ in entity_types)
        conditions.append(f"entity_type IN ({ph})")
        params.extend(entity_types)
    min_tokens = proposed.get('applicable_min_tokens', 1)
    if min_tokens > 1:
        conditions.append("num_tokens >= ?")
        params.append(min_tokens)
    min_len = proposed.get('applicable_min_name_length', 1)
    if min_len > 1:
        conditions.append("name_length >= ?")
        params.append(min_len)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    async with db.execute(
        f"SELECT cleaned_name FROM watchlist_entries {where} ORDER BY RANDOM() LIMIT ?",
        params + [n],
    ) as cur:
        rows = await cur.fetchall()
    return [r[0] for r in rows]


# ── Public entry point ──────────────────────────────────────────────────────────

async def handle_message(message: dict, db: aiosqlite.Connection) -> dict:
    """
    Process one turn of the chatbot conversation.

    message = {
        "session_id": "<optional>",
        "content": "<user text>"
    }
    Returns {
        "session_id": "...",
        "reply": "...",
        "stage": "...",
        "proposed_type": {...} | null,
        "examples": [...],
    }
    """
    session_id = message.get('session_id') or str(uuid.uuid4())
    user_content = (message.get('content') or '').strip()

    if not user_content:
        return {
            'session_id': session_id,
            'reply': "Hello! Describe a new name variation test case type and I'll help you define it. For example: \"Create a test where you reverse each token in the name and add a hyphen between them.\"",
            'stage': 'new',
            'proposed_type': None,
            'examples': [],
        }

    # Load existing session
    session = await _load_session(session_id, db)

    # Pre-load sample names for example generation
    proposed_for_samples = session.get('proposed_type') or {}
    if user_content.lower().strip() not in _CONFIRM_WORDS:
        # For extract/refine, we'll load samples after the type is proposed
        # But we need at least some fallback names for the generate_examples node
        sample_names = await _load_sample_names(
            proposed_for_samples or {'applicable_entity_types': ['individual', 'entity'], 'applicable_min_tokens': 2},
            db,
        )
    else:
        sample_names = []

    # Build graph input state
    graph_state: ChatState = {
        'session_id': session_id,
        'user_message': user_content,
        'stage': session['stage'],
        'proposed_type': session.get('proposed_type'),
        'examples': session.get('examples', []),
        'iteration': session.get('iteration', 0),
        'response': '',
        'sample_names': sample_names,
    }

    # Run graph
    try:
        result = await get_graph().ainvoke(graph_state)
    except Exception as exc:
        return {
            'session_id': session_id,
            'reply': f"An error occurred: {exc}. Please try again.",
            'stage': session['stage'],
            'proposed_type': session.get('proposed_type'),
            'examples': session.get('examples', []),
        }

    new_stage = result.get('stage', session['stage'])
    proposed_type = result.get('proposed_type', session.get('proposed_type'))
    examples = result.get('examples', session.get('examples', []))

    # If the proposed type is new or refined (extract/refine ran), reload samples with correct constraints
    # and re-run generate_examples if needed (already done inside the graph for these branches)

    saved_type_id = None
    if new_stage == 'saved' and proposed_type:
        saved_type_id = await _save_custom_type(proposed_type, db)
        # After saving, reset stage to 'new' so user can describe another
        new_stage = 'new'
        proposed_type = None
        examples = []

    # Update message history
    messages = session.get('messages', [])
    messages.append({'role': 'user', 'content': user_content})
    messages.append({'role': 'assistant', 'content': result.get('response', '')})
    # Keep last 20 messages to avoid unbounded growth
    messages = messages[-20:]

    # Persist session
    await _save_session(session_id, {
        'stage': new_stage,
        'proposed_type': proposed_type,
        'examples': examples,
        'iteration': result.get('iteration', session.get('iteration', 0)),
        'messages': messages,
    }, db)

    return {
        'session_id': session_id,
        'reply': result.get('response', ''),
        'stage': new_stage,
        'proposed_type': proposed_type,
        'examples': examples,
        'saved_type_id': saved_type_id,
    }
