"""
export_service.py — Four export formats for generated test cases.

Formats:
  1. Excel  — openpyxl workbook: Test Cases sheet + Summary sheet
  2. pacs.008 — ISO 20022 XML Customer Credit Transfer (ZIP of chunks)
  3. pacs.009 — ISO 20022 XML FI Credit Transfer (ZIP of chunks)
  4. FUF    — SWIFT MT103-compatible Firco Universal Format (plain text)

For SWIFT formats, test names are rotated across several message fields
so the export exercises all fields a screening system might scan.
"""
from __future__ import annotations

import io
import textwrap
import zipfile
from datetime import date, datetime
from typing import Optional

import aiosqlite
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Helpers ────────────────────────────────────────────────────────────────────

def _xmlesc(text: str) -> str:
    """Minimal XML escape for content values."""
    return (
        str(text)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
    )


async def _load_cases(
    db: aiosqlite.Connection,
    expected_result: Optional[str] = None,
    entity_type: Optional[str] = None,
    limit: int = 50_000,
) -> list[dict]:
    conditions = []
    params: list = []
    if expected_result:
        conditions.append("expected_result = ?")
        params.append(expected_result)
    if entity_type:
        conditions.append("entity_type = ?")
        params.append(entity_type)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    async with db.execute(
        f"""SELECT test_case_id, test_case_type, watchlist, sub_watchlist,
                   cleaned_original_name, original_original_name, culture_nationality,
                   test_name, primary_aka, entity_type, num_tokens, name_length,
                   expected_result, expected_result_rationale, created_at
            FROM test_cases {where}
            ORDER BY test_case_id
            LIMIT ?""",
        params + [limit],
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


# ── PACS.008 field placement rotation ─────────────────────────────────────────
# Test names cycle through these debtor/creditor fields to validate that
# the screening system screens ALL relevant message fields.

PACS008_PLACEMENTS = [
    'Debtor Name',
    'Creditor Name',
    'Debtor Address Line',
    'Ultimate Debtor Name',
]

PACS009_PLACEMENTS = [
    'Instructing Agent Name',
    'Ordering Institution Name',
    'Beneficiary Institution Name',
    'Intermediary Agent Name',
]

FUF_PLACEMENTS = [
    'Field :50K (Ordering Customer)',
    'Field :59 (Beneficiary Customer)',
    'Field :70 (Remittance Info)',
    'Field :50K Address Line',
]

# Dummy names/values for non-test-name fields
_DUMMY_DEBTOR = 'ACME TRADING CORPORATION'
_DUMMY_CREDITOR = 'GLOBAL PARTNERS LIMITED'
_DUMMY_INSTITUTION = 'NORTHERN BANK PLC'
_TODAY = date.today().isoformat()
_NOW_ISO = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
_VALUE_DATE = date.today().strftime('%y%m%d')


# ── 1. EXCEL EXPORT ────────────────────────────────────────────────────────────

async def export_names_only(
    db: aiosqlite.Connection,
    expected_result: Optional[str] = None,
    entity_type: Optional[str] = None,
) -> bytes:
    """Excel workbook: Test Cases sheet + Summary sheet."""
    cases = await _load_cases(db, expected_result, entity_type)

    wb = openpyxl.Workbook()

    # ── Sheet 1: Test Cases ──────────────────────────────────────────────────
    ws = wb.active
    ws.title = 'Test Cases'

    header_fill = PatternFill(fill_type='solid', fgColor='1E3A5F')
    header_font = Font(bold=True, color='FFFFFF', size=10)
    thin = Side(style='thin', color='D0D0D0')
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    headers = [
        'Test Case ID', 'Test Case Type', 'Watchlist', 'Sub-Watchlist',
        'Cleaned Original Name', 'Original Original Name', 'Culture/Nationality',
        'Test Name', 'Primary/AKA', 'Entity Type', '# Tokens', 'Name Length',
        'Expected Result', 'Expected Result Rationale',
    ]
    col_widths = [24, 36, 14, 20, 36, 36, 18, 36, 10, 12, 9, 12, 15, 60]

    for col, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border = border
        ws.column_dimensions[get_column_letter(col)].width = w

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = 'A2'

    hit_fill = PatternFill(fill_type='solid', fgColor='D1FAE5')  # light green
    miss_fill = PatternFill(fill_type='solid', fgColor='FEE2E2')  # light red

    for row_i, c in enumerate(cases, 2):
        values = [
            c['test_case_id'], c['test_case_type'], c['watchlist'],
            c.get('sub_watchlist') or '', c['cleaned_original_name'],
            c['original_original_name'], c.get('culture_nationality') or '',
            c['test_name'], c.get('primary_aka') or '', c.get('entity_type') or '',
            c.get('num_tokens') or '', c.get('name_length') or '',
            c['expected_result'], c.get('expected_result_rationale') or '',
        ]
        result_fill = hit_fill if c['expected_result'] == 'HIT' else miss_fill
        for col, v in enumerate(values, 1):
            cell = ws.cell(row=row_i, column=col, value=v)
            cell.border = border
            cell.alignment = Alignment(vertical='center', wrap_text=(col == len(headers)))
            if col == 13:  # Expected Result column
                cell.fill = result_fill
                cell.font = Font(bold=True, size=10)

    # ── Sheet 2: Summary ─────────────────────────────────────────────────────
    ws2 = wb.create_sheet('Summary')
    ws2.column_dimensions['A'].width = 28
    ws2.column_dimensions['B'].width = 16

    def _summary_header(row, title):
        cell = ws2.cell(row=row, column=1, value=title)
        cell.font = Font(bold=True, size=11, color='1E3A5F')
        ws2.merge_cells(start_row=row, start_column=1, end_row=row, end_column=2)

    def _row(row, label, value):
        ws2.cell(row=row, column=1, value=label)
        ws2.cell(row=row, column=2, value=value).alignment = Alignment(horizontal='right')

    _summary_header(1, 'Export Summary')
    _row(2, 'Total test cases', len(cases))
    _row(3, 'Generated at', datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'))
    if expected_result:
        _row(4, 'Filter: Expected result', expected_result)
    if entity_type:
        _row(5, 'Filter: Entity type', entity_type)

    _summary_header(7, 'By Expected Result')
    by_result: dict[str, int] = {}
    for c in cases:
        by_result[c['expected_result']] = by_result.get(c['expected_result'], 0) + 1
    for ri, (k, v) in enumerate(sorted(by_result.items()), 8):
        _row(ri, k, v)

    row_base = 8 + len(by_result) + 2
    _summary_header(row_base, 'By Entity Type')
    by_et: dict[str, int] = {}
    for c in cases:
        et = c.get('entity_type') or 'unknown'
        by_et[et] = by_et.get(et, 0) + 1
    for ri, (k, v) in enumerate(sorted(by_et.items(), key=lambda x: -x[1]), row_base + 1):
        _row(ri, k, v)

    row_base2 = row_base + 1 + len(by_et) + 2
    _summary_header(row_base2, 'By Watchlist')
    by_wl: dict[str, int] = {}
    for c in cases:
        wl = c.get('watchlist') or 'unknown'
        by_wl[wl] = by_wl.get(wl, 0) + 1
    for ri, (k, v) in enumerate(sorted(by_wl.items(), key=lambda x: -x[1]), row_base2 + 1):
        _row(ri, k, v)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ── 2. PACS.008 EXPORT ─────────────────────────────────────────────────────────

def _pacs008_transaction(tc: dict, placement: str, seq: int) -> str:
    """Build one <CdtTrfTxInf> element string for pacs.008."""
    name = _xmlesc(tc['test_name'])
    dummy_d = _xmlesc(_DUMMY_DEBTOR)
    dummy_c = _xmlesc(_DUMMY_CREDITOR)
    tc_id = _xmlesc(tc['test_case_id'])

    # Assign test name to the appropriate field
    debtor_name = name if placement == 'Debtor Name' else dummy_d
    creditor_name = name if placement == 'Creditor Name' else dummy_c
    debtor_addr = name if placement == 'Debtor Address Line' else '123 Main Street'
    ult_debtor = f'<UltmtDbtr><Nm>{name}</Nm></UltmtDbtr>' if placement == 'Ultimate Debtor Name' else ''

    iban_d = f'GB29NWBK6016{seq:07d}'[:22]
    iban_c = f'GB29BARC6016{seq:07d}'[:22]

    return textwrap.dedent(f"""\
        <CdtTrfTxInf>
          <PmtId>
            <InstrId>{tc_id}</InstrId>
            <EndToEndId>{tc_id}</EndToEndId>
          </PmtId>
          <IntrBkSttlmAmt Ccy="USD">1000.00</IntrBkSttlmAmt>
          <ChrgBr>SHAR</ChrgBr>
          <Dbtr>
            <Nm>{debtor_name}</Nm>
            <PstlAdr>
              <Ctry>US</Ctry>
              <AdrLine>{debtor_addr}</AdrLine>
            </PstlAdr>
          </Dbtr>
          <DbtrAcct><Id><IBAN>{iban_d}</IBAN></Id></DbtrAcct>
          <DbtrAgt><FinInstnId><BICFI>NWBKGB2L</BICFI></FinInstnId></DbtrAgt>
          <CdtrAgt><FinInstnId><BICFI>BARCGB22</BICFI></FinInstnId></CdtrAgt>
          <Cdtr>
            <Nm>{creditor_name}</Nm>
            <PstlAdr><Ctry>GB</Ctry></PstlAdr>
          </Cdtr>
          <CdtrAcct><Id><IBAN>{iban_c}</IBAN></Id></CdtrAcct>
          {ult_debtor}
        </CdtTrfTxInf>""")


def _pacs008_document(cases: list[dict], msg_id: str) -> str:
    ns = 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.10'
    n = len(cases)
    transactions = []
    for i, tc in enumerate(cases):
        placement = PACS008_PLACEMENTS[i % len(PACS008_PLACEMENTS)]
        transactions.append(_pacs008_transaction(tc, placement, i + 1))

    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <Document xmlns="{ns}">
          <FIToFICstmrCdtTrf>
            <GrpHdr>
              <MsgId>{_xmlesc(msg_id)}</MsgId>
              <CreDtTm>{_NOW_ISO}</CreDtTm>
              <NbOfTxs>{n}</NbOfTxs>
              <TtlIntrBkSttlmAmt Ccy="USD">{n * 1000:.2f}</TtlIntrBkSttlmAmt>
              <IntrBkSttlmDt>{_TODAY}</IntrBkSttlmDt>
              <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
            </GrpHdr>
        {chr(10).join(transactions)}
          </FIToFICstmrCdtTrf>
        </Document>""")


async def export_pacs008(
    db: aiosqlite.Connection,
    expected_result: Optional[str] = None,
    entity_type: Optional[str] = None,
    chunk_size: int = 1000,
) -> bytes:
    """ZIP of pacs.008 XML files, one per chunk of test cases."""
    cases = await _load_cases(db, expected_result, entity_type)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for chunk_i, start in enumerate(range(0, max(len(cases), 1), chunk_size), 1):
            chunk = cases[start:start + chunk_size]
            if not chunk:
                break
            msg_id = f'OFAC-TEST-{_TODAY}-PACS008-{chunk_i:03d}'
            xml_str = _pacs008_document(chunk, msg_id)
            zf.writestr(f'pacs008_{chunk_i:03d}.xml', xml_str.encode('utf-8'))

        # Include a README
        readme = (
            f"OFAC Screening Validation — pacs.008 Test Data\n"
            f"Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}\n"
            f"Total messages: {len(cases)}\n"
            f"Files: {max(1, (len(cases) + chunk_size - 1) // chunk_size)}\n\n"
            f"Field placement rotation:\n"
            + '\n'.join(f"  {i+1}. {p}" for i, p in enumerate(PACS008_PLACEMENTS))
            + '\n\nISO 20022 pacs.008.001.10\n'
        )
        zf.writestr('README.txt', readme.encode('utf-8'))

    buf.seek(0)
    return buf.read()


# ── 3. PACS.009 EXPORT ─────────────────────────────────────────────────────────

def _pacs009_transaction(tc: dict, placement: str, seq: int) -> str:
    """Build one <CdtTrfTxInf> element string for pacs.009."""
    name = _xmlesc(tc['test_name'])
    dummy_inst = _xmlesc(_DUMMY_INSTITUTION)
    tc_id = _xmlesc(tc['test_case_id'])

    instg_nm = f'<Nm>{name}</Nm>' if placement == 'Instructing Agent Name' else ''
    ord_nm = f'<Nm>{name}</Nm>' if placement == 'Ordering Institution Name' else f'<Nm>{dummy_inst}</Nm>'
    bene_nm = f'<Nm>{name}</Nm>' if placement == 'Beneficiary Institution Name' else f'<Nm>{dummy_inst}</Nm>'
    interm = (
        f'<IntrmyAgt1><FinInstnId><BICFI>MIDLGB22</BICFI><Nm>{name}</Nm></FinInstnId></IntrmyAgt1>'
        if placement == 'Intermediary Agent Name' else ''
    )

    return textwrap.dedent(f"""\
        <CdtTrfTxInf>
          <PmtId>
            <InstrId>{tc_id}</InstrId>
            <EndToEndId>{tc_id}</EndToEndId>
          </PmtId>
          <IntrBkSttlmAmt Ccy="USD">1000.00</IntrBkSttlmAmt>
          <ChrgBr>SHAR</ChrgBr>
          <InstgAgt>
            <FinInstnId><BICFI>NWBKGB2L</BICFI>{instg_nm}</FinInstnId>
          </InstgAgt>
          <InstdAgt>
            <FinInstnId><BICFI>BARCGB22</BICFI></FinInstnId>
          </InstdAgt>
          {interm}
          <Dbtr>
            <FinInstnId><BICFI>TESTBIC1</BICFI>{ord_nm}</FinInstnId>
          </Dbtr>
          <Cdtr>
            <FinInstnId><BICFI>TESTBIC2</BICFI>{bene_nm}</FinInstnId>
          </Cdtr>
          <DbtrAcct><Id><IBAN>GB29NWBK{seq:014d}</IBAN></Id></DbtrAcct>
          <CdtrAcct><Id><IBAN>GB29BARC{seq:014d}</IBAN></Id></CdtrAcct>
        </CdtTrfTxInf>""")


def _pacs009_document(cases: list[dict], msg_id: str) -> str:
    ns = 'urn:iso:std:iso:20022:tech:xsd:pacs.009.001.10'
    n = len(cases)
    transactions = []
    for i, tc in enumerate(cases):
        placement = PACS009_PLACEMENTS[i % len(PACS009_PLACEMENTS)]
        transactions.append(_pacs009_transaction(tc, placement, i + 1))

    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <Document xmlns="{ns}">
          <FIToFIFICdtTrf>
            <GrpHdr>
              <MsgId>{_xmlesc(msg_id)}</MsgId>
              <CreDtTm>{_NOW_ISO}</CreDtTm>
              <NbOfTxs>{n}</NbOfTxs>
              <TtlIntrBkSttlmAmt Ccy="USD">{n * 1000:.2f}</TtlIntrBkSttlmAmt>
              <IntrBkSttlmDt>{_TODAY}</IntrBkSttlmDt>
              <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
            </GrpHdr>
        {chr(10).join(transactions)}
          </FIToFIFICdtTrf>
        </Document>""")


async def export_pacs009(
    db: aiosqlite.Connection,
    expected_result: Optional[str] = None,
    entity_type: Optional[str] = None,
    chunk_size: int = 1000,
) -> bytes:
    """ZIP of pacs.009 XML files, one per chunk of test cases."""
    cases = await _load_cases(db, expected_result, entity_type)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for chunk_i, start in enumerate(range(0, max(len(cases), 1), chunk_size), 1):
            chunk = cases[start:start + chunk_size]
            if not chunk:
                break
            msg_id = f'OFAC-TEST-{_TODAY}-PACS009-{chunk_i:03d}'
            xml_str = _pacs009_document(chunk, msg_id)
            zf.writestr(f'pacs009_{chunk_i:03d}.xml', xml_str.encode('utf-8'))

        readme = (
            f"OFAC Screening Validation — pacs.009 Test Data\n"
            f"Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}\n"
            f"Total messages: {len(cases)}\n\n"
            f"Field placement rotation:\n"
            + '\n'.join(f"  {i+1}. {p}" for i, p in enumerate(PACS009_PLACEMENTS))
            + '\n\nISO 20022 pacs.009.001.10\n'
        )
        zf.writestr('README.txt', readme.encode('utf-8'))

    buf.seek(0)
    return buf.read()


# ── 4. FUF / SWIFT MT103 EXPORT ────────────────────────────────────────────────

def _fuf_message(tc: dict, placement: str, seq: int) -> str:
    """
    Build one FUF-compatible SWIFT MT103 message block.
    Field placement:
      :50K  → Ordering Customer (Debtor) — main screening target in inter-bank payments
      :59   → Beneficiary Customer (Creditor)
      :70   → Remittance Information (some systems also screen this field)
      :50K address line → Address field of the Ordering Customer
    """
    name = tc['test_name']
    tc_id = tc['test_case_id']

    # Truncate to 35 chars per SWIFT line limit (MT103 field width)
    def _sw(s: str) -> str:
        return s[:35].replace('\n', ' ').replace('{', '(').replace('}', ')')

    name_sw = _sw(name)

    if placement == 'Field :50K (Ordering Customer)':
        field_50k = f':50K:/ACCT{seq:09d}\n{name_sw}'
        field_59 = f':59:/ACCT{seq + 1:09d}\n{_sw(_DUMMY_CREDITOR)}'
        field_70 = ':70:/RFB/TEST PAYMENT REF'
    elif placement == 'Field :59 (Beneficiary Customer)':
        field_50k = f':50K:/ACCT{seq:09d}\n{_sw(_DUMMY_DEBTOR)}'
        field_59 = f':59:/ACCT{seq + 1:09d}\n{name_sw}'
        field_70 = ':70:/RFB/TEST PAYMENT REF'
    elif placement == 'Field :70 (Remittance Info)':
        field_50k = f':50K:/ACCT{seq:09d}\n{_sw(_DUMMY_DEBTOR)}'
        field_59 = f':59:/ACCT{seq + 1:09d}\n{_sw(_DUMMY_CREDITOR)}'
        field_70 = f':70:/INV/{name_sw}'
    else:  # :50K Address Line
        field_50k = f':50K:/ACCT{seq:09d}\n{_sw(_DUMMY_DEBTOR)}\n{name_sw}'
        field_59 = f':59:/ACCT{seq + 1:09d}\n{_sw(_DUMMY_CREDITOR)}'
        field_70 = ':70:/RFB/TEST PAYMENT REF'

    return (
        f'{{1:F01NWBKGB2LAXXX{seq:010d}}}'
        f'{{2:I103BARCGB22XXXXN}}'
        f'{{3:{{108:{tc_id[:16]}}}}}'
        f'{{4:\n'
        f':20:{tc_id[:16]}\n'
        f':23B:CRED\n'
        f':32A:{_VALUE_DATE}USD1000,00\n'
        f'{field_50k}\n'
        f':57A:BARCGB22\n'
        f'{field_59}\n'
        f'{field_70}\n'
        f':71A:SHA\n'
        f'-}}\n'
        f'{{5:{{CHK:{seq:012X}}}}}\n'
    )


async def export_fuf(
    db: aiosqlite.Connection,
    expected_result: Optional[str] = None,
    entity_type: Optional[str] = None,
) -> bytes:
    """Single FUF (SWIFT MT103-compatible) text file containing all test messages."""
    cases = await _load_cases(db, expected_result, entity_type)
    lines = [
        f'FUF EXPORT — OFAC Screening Validation Platform\n'
        f'Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}\n'
        f'Total messages: {len(cases)}\n'
        f'Format: SWIFT MT103 / Firco Universal Format\n'
        f'Field rotation: {" | ".join(FUF_PLACEMENTS)}\n'
        f'{"=" * 80}\n\n'
    ]
    for i, tc in enumerate(cases):
        placement = FUF_PLACEMENTS[i % len(FUF_PLACEMENTS)]
        lines.append(_fuf_message(tc, placement, i + 1))
    return ''.join(lines).encode('utf-8')
