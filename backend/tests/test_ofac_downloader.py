"""
Tests for OFAC SDN XML parser and cleaner.
Run from backend/ directory: python -m pytest tests/ -v
"""
import pytest
from app.services.list_downloader import parse_ofac_xml
from app.services.list_cleaner import clean_name, count_tokens, detect_recently_modified, normalize_entity_type

# ── Minimal OFAC SDN XML fixture ───────────────────────────────────────────────

SAMPLE_SDN_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<sdnList xmlns="http://tempuri.org/sdnList.xsd">
  <publshInformation>
    <Publish_Date>03/01/2024</Publish_Date>
    <Record_Count>3</Record_Count>
  </publshInformation>
  <sdnEntry>
    <uid>100</uid>
    <lastName>AL-QAIDA</lastName>
    <firstName></firstName>
    <sdnType>Entity</sdnType>
    <programList>
      <program>SDGT</program>
      <program>IRGC</program>
    </programList>
    <akaList>
      <aka>
        <uid>101</uid>
        <type>a.k.a.</type>
        <category>strong</category>
        <lastName>AL QAEDA</lastName>
        <firstName></firstName>
      </aka>
      <aka>
        <uid>102</uid>
        <type>a.k.a.</type>
        <category>weak</category>
        <lastName>THE BASE</lastName>
        <firstName></firstName>
      </aka>
    </akaList>
  </sdnEntry>
  <sdnEntry>
    <uid>200</uid>
    <lastName>KHAMENEI</lastName>
    <firstName>ALI</firstName>
    <sdnType>Individual</sdnType>
    <programList>
      <program>IRAN</program>
    </programList>
    <akaList/>
  </sdnEntry>
  <sdnEntry>
    <uid>300</uid>
    <lastName>OCEAN NAVIGATOR</lastName>
    <firstName></firstName>
    <sdnType>Vessel</sdnType>
    <programList>
      <program>IRAN</program>
    </programList>
    <akaList/>
  </sdnEntry>
</sdnList>
"""


# ── Parser tests ───────────────────────────────────────────────────────────────

def test_ofac_parse_count():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    # 1 entity primary + 2 AKAs + 1 individual primary + 1 vessel primary = 5
    assert len(entries) == 5


def test_ofac_primary_name():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    primaries = [e for e in entries if e["primary_aka"] == "primary"]
    names = {e["original_name"] for e in primaries}
    assert "AL-QAIDA" in names
    assert "ALI KHAMENEI" in names
    assert "OCEAN NAVIGATOR" in names


def test_ofac_aka_names():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    akas = [e for e in entries if e["primary_aka"] == "aka"]
    aka_names = {e["original_name"] for e in akas}
    assert "AL QAEDA" in aka_names
    assert "THE BASE" in aka_names


def test_ofac_entity_types():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    by_name = {e["original_name"]: e["entity_type"] for e in entries}
    assert by_name["AL-QAIDA"] == "entity"
    assert by_name["ALI KHAMENEI"] == "individual"
    assert by_name["OCEAN NAVIGATOR"] == "vessel"


def test_ofac_programs():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    alqaida = next(e for e in entries if e["original_name"] == "AL-QAIDA")
    assert "SDGT" in alqaida["sanctions_program"]
    assert "IRGC" in alqaida["sanctions_program"]


def test_ofac_watchlist_key():
    entries = parse_ofac_xml(SAMPLE_SDN_XML, "OFAC_SDN")
    for e in entries:
        assert e["watchlist"] == "OFAC_SDN"


# ── Cleaner tests ──────────────────────────────────────────────────────────────

def test_clean_name_all_caps():
    assert clean_name("JOHN SMITH") == "John Smith"


def test_clean_name_already_mixed():
    assert clean_name("McDonald's Inc.") == "McDonald's Inc."


def test_clean_name_nfc():
    # Precomposed vs decomposed 'é'
    decomposed = "Re\u0301mi"  # e + combining acute
    composed = "R\u00e9mi"
    assert clean_name(decomposed) == clean_name(composed)


def test_clean_name_collapse_spaces():
    assert clean_name("  John   Smith  ") == "John Smith"


def test_clean_name_arabic_preserved():
    arabic = "أسامة بن لادن"
    result = clean_name(arabic)
    assert len(result) > 0
    # Should not mangle Arabic characters
    assert "أ" in result or "ا" in result


def test_count_tokens():
    assert count_tokens("John Smith") == 2
    assert count_tokens("Ali") == 1
    assert count_tokens("") == 0
    assert count_tokens("  ") == 0


def test_detect_recently_modified_recent():
    from datetime import datetime, timezone, timedelta
    recent = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    assert detect_recently_modified(recent) is True


def test_detect_recently_modified_old():
    assert detect_recently_modified("2000-01-01") is False


def test_detect_recently_modified_none():
    assert detect_recently_modified(None) is False


def test_normalize_entity_type():
    assert normalize_entity_type("Individual") == "individual"
    assert normalize_entity_type("ENTITY") == "entity"
    assert normalize_entity_type("vessel") == "vessel"
    assert normalize_entity_type("") == "unknown"
    assert normalize_entity_type("Organisation") == "entity"
