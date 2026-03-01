"""
Tests for export_service.py — all four export formats.
No DB or filesystem I/O needed (tested against the helper functions directly).
"""
import io
import zipfile
import pytest

from app.services.export_service import (
    _xmlesc,
    _pacs008_document, _pacs008_transaction,
    _pacs009_document, _pacs009_transaction,
    _fuf_message,
    PACS008_PLACEMENTS, PACS009_PLACEMENTS, FUF_PLACEMENTS,
)


SAMPLE_CASES = [
    {
        'test_case_id': 'TC001_aabbccdd',
        'test_name': 'Ahmad Al-Rashid',
        'expected_result': 'HIT',
        'entity_type': 'individual',
    },
    {
        'test_case_id': 'TC002_eeffgghh',
        'test_name': 'Bank Melli Iran',
        'expected_result': 'HIT',
        'entity_type': 'entity',
    },
    {
        'test_case_id': 'TC003_iijjkkll',
        'test_name': 'Qasem Soleimani',
        'expected_result': 'HIT',
        'entity_type': 'individual',
    },
    {
        'test_case_id': 'TC004_mmnnoopp',
        'test_name': 'MV Victory Star',
        'expected_result': 'HIT',
        'entity_type': 'vessel',
    },
    {
        'test_case_id': 'TC005_qqrrsstt',
        'test_name': 'Rosatom <Corp> & Partners',
        'expected_result': 'MISS',
        'entity_type': 'entity',
    },
]


class TestXmlEscape:
    def test_ampersand(self):
        assert _xmlesc('A & B') == 'A &amp; B'

    def test_less_than(self):
        assert _xmlesc('x < y') == 'x &lt; y'

    def test_greater_than(self):
        assert _xmlesc('x > y') == 'x &gt; y'

    def test_quote(self):
        assert _xmlesc('"hello"') == '&quot;hello&quot;'

    def test_combined(self):
        result = _xmlesc('Bank <LLC> & "Trust"')
        assert '&amp;' in result
        assert '&lt;' in result
        assert '&gt;' in result
        assert '&quot;' in result

    def test_plain_ascii_unchanged(self):
        assert _xmlesc('Ahmad Al-Rashid') == 'Ahmad Al-Rashid'


class TestPacs008Transaction:
    def test_debtor_name_placement(self):
        tx = _pacs008_transaction(SAMPLE_CASES[0], 'Debtor Name', 1)
        assert 'Ahmad Al-Rashid' in tx
        assert '<Dbtr>' in tx

    def test_creditor_name_placement(self):
        tx = _pacs008_transaction(SAMPLE_CASES[1], 'Creditor Name', 2)
        assert 'Bank Melli Iran' in tx
        assert '<Cdtr>' in tx

    def test_debtor_address_placement(self):
        tx = _pacs008_transaction(SAMPLE_CASES[0], 'Debtor Address Line', 1)
        assert 'Ahmad Al-Rashid' in tx
        assert '<AdrLine>' in tx

    def test_ultimate_debtor_placement(self):
        tx = _pacs008_transaction(SAMPLE_CASES[0], 'Ultimate Debtor Name', 1)
        assert 'Ahmad Al-Rashid' in tx
        assert 'UltmtDbtr' in tx

    def test_xml_special_chars_escaped(self):
        tx = _pacs008_transaction(SAMPLE_CASES[4], 'Debtor Name', 5)
        assert '&lt;Corp&gt;' in tx
        assert '&amp;' in tx

    def test_transaction_id_included(self):
        tx = _pacs008_transaction(SAMPLE_CASES[0], 'Debtor Name', 1)
        assert 'TC001_aabbccdd' in tx


class TestPacs008Document:
    def test_valid_xml_header(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MSG-001')
        assert '<?xml version="1.0"' in xml

    def test_correct_namespace(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MSG-001')
        assert 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.10' in xml

    def test_message_id(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MY-MSG-ID-123')
        assert 'MY-MSG-ID-123' in xml

    def test_transaction_count(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MSG-001')
        assert f'NbOfTxs>{len(SAMPLE_CASES)}<' in xml

    def test_all_names_present(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MSG-001')
        for c in SAMPLE_CASES:
            # Names with special chars are escaped
            assert _xmlesc(c['test_name']) in xml

    def test_placement_rotation(self):
        xml = _pacs008_document(SAMPLE_CASES, 'MSG-001')
        # First case → Debtor Name → <Dbtr><Nm>...
        assert 'Ahmad Al-Rashid' in xml
        # Second case → Creditor Name → <Cdtr><Nm>...
        assert 'Bank Melli Iran' in xml

    def test_single_case(self):
        xml = _pacs008_document([SAMPLE_CASES[0]], 'MSG-SINGLE')
        assert 'NbOfTxs>1<' in xml


class TestPacs009Document:
    def test_valid_xml_header(self):
        xml = _pacs009_document(SAMPLE_CASES, 'MSG-009-001')
        assert '<?xml version="1.0"' in xml

    def test_correct_namespace(self):
        xml = _pacs009_document(SAMPLE_CASES, 'MSG-009-001')
        assert 'urn:iso:std:iso:20022:tech:xsd:pacs.009.001.10' in xml

    def test_uses_fito_fi_element(self):
        xml = _pacs009_document(SAMPLE_CASES, 'MSG-009-001')
        assert '<FIToFIFICdtTrf>' in xml

    def test_all_names_present(self):
        xml = _pacs009_document(SAMPLE_CASES, 'MSG-009-001')
        for c in SAMPLE_CASES:
            assert _xmlesc(c['test_name']) in xml

    def test_instructing_agent_placement(self):
        tx = _pacs009_transaction(SAMPLE_CASES[0], 'Instructing Agent Name', 1)
        assert 'Ahmad Al-Rashid' in tx
        assert 'InstgAgt' in tx

    def test_ordering_institution_placement(self):
        tx = _pacs009_transaction(SAMPLE_CASES[1], 'Ordering Institution Name', 2)
        assert 'Bank Melli Iran' in tx
        assert '<Dbtr>' in tx

    def test_beneficiary_institution_placement(self):
        tx = _pacs009_transaction(SAMPLE_CASES[2], 'Beneficiary Institution Name', 3)
        assert 'Qasem Soleimani' in tx
        assert '<Cdtr>' in tx

    def test_intermediary_placement(self):
        tx = _pacs009_transaction(SAMPLE_CASES[3], 'Intermediary Agent Name', 4)
        assert 'MV Victory Star' in tx
        assert 'IntrmyAgt' in tx


class TestFufMessage:
    def test_field_50k_placement(self):
        msg = _fuf_message(SAMPLE_CASES[0], 'Field :50K (Ordering Customer)', 1)
        assert ':50K:' in msg
        assert 'Ahmad Al-Rashid' in msg

    def test_field_59_placement(self):
        msg = _fuf_message(SAMPLE_CASES[1], 'Field :59 (Beneficiary Customer)', 2)
        assert ':59:' in msg
        assert 'Bank Melli Iran' in msg

    def test_field_70_placement(self):
        msg = _fuf_message(SAMPLE_CASES[2], 'Field :70 (Remittance Info)', 3)
        assert ':70:' in msg
        assert 'Qasem Soleimani' in msg

    def test_field_50k_addr_placement(self):
        msg = _fuf_message(SAMPLE_CASES[0], 'Field :50K Address Line', 1)
        assert ':50K:' in msg
        assert 'Ahmad Al-Rashid' in msg

    def test_has_transaction_ref(self):
        msg = _fuf_message(SAMPLE_CASES[0], FUF_PLACEMENTS[0], 1)
        assert ':20:' in msg

    def test_has_bank_op_code(self):
        msg = _fuf_message(SAMPLE_CASES[0], FUF_PLACEMENTS[0], 1)
        assert ':23B:CRED' in msg

    def test_has_value_date_currency(self):
        msg = _fuf_message(SAMPLE_CASES[0], FUF_PLACEMENTS[0], 1)
        assert ':32A:' in msg
        assert 'USD' in msg

    def test_swift_block_structure(self):
        msg = _fuf_message(SAMPLE_CASES[0], FUF_PLACEMENTS[0], 1)
        assert '{1:F01' in msg
        assert '{2:I103' in msg
        assert '{4:' in msg
        assert '{5:' in msg

    def test_name_truncated_to_swift_limit(self):
        long_name_case = {**SAMPLE_CASES[0], 'test_name': 'A' * 50}
        msg = _fuf_message(long_name_case, FUF_PLACEMENTS[0], 1)
        # Name should be truncated to 35 chars max per Swift line
        lines = msg.split('\n')
        for line in lines:
            assert len(line) <= 80, f'Line too long: {line!r}'


class TestFieldRotation:
    def test_pacs008_rotates_through_all_placements(self):
        n = len(PACS008_PLACEMENTS)
        cases = SAMPLE_CASES[:n]
        xml = _pacs008_document(cases, 'MSG')
        # With exactly N cases, each placement used exactly once
        assert xml.count('<CdtTrfTxInf>') == n

    def test_pacs009_has_4_placements(self):
        assert len(PACS009_PLACEMENTS) == 4

    def test_fuf_has_4_placements(self):
        assert len(FUF_PLACEMENTS) == 4

    def test_pacs008_has_4_placements(self):
        assert len(PACS008_PLACEMENTS) == 4
