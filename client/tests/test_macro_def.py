from decimal import Decimal
from dwh_migration_client.macro_processor_ofr import MacroDef


def test_guess_type():
    a = MacroDef("test_int", "5")
    a.guess_macro_type()
    assert a.macro_type == "integer"

    a = MacroDef("test_dect", "5.0")
    a.guess_macro_type()
    assert a.macro_type == "decimal"

    a = MacroDef("test_dt", "2012-10-18")
    a.guess_macro_type()
    assert a.macro_type == "datetime:%Y-%m-%d"

    a = MacroDef("test_dt", "9999-12-31 23:59:59")
    a.guess_macro_type()
    assert a.macro_type == "datetime:%Y-%m-%d %H:%M:%S"

    a = MacroDef("test_dt", "31/12/9999")
    a.guess_macro_type()
    assert a.macro_type == "datetime:%d/%m/%Y"

    a = MacroDef("test_dt", "2000/01/01 00:00:00")
    a.guess_macro_type()
    assert a.macro_type == "datetime:%Y/%m/%d %H:%M:%S"

    a = MacroDef("test_dt", "2000/01*01 80:80:80")
    a.guess_macro_type()
    assert a.macro_type == "string"
    assert a.quote == False

    a = MacroDef("test_dt", "'azerty'")
    a.guess_macro_type()
    assert a.macro_type == "string"
    assert a.quote == True

    a = MacroDef("test_dt", "COM_CAP_VM_MM2_CY2")
    a.guess_macro_type()
    assert a.macro_type == "database"


def test_try_uncollie():
    m_i = MacroDef("test_int", "5")
    u_i = m_i.try_uncollide_int_or_decimal(int(m_i.expansion))

    m_d = MacroDef("test_dect", "5.2")
    u_d = m_d.try_uncollide_int_or_decimal(Decimal(m_d.expansion))

    m_dt = MacroDef("test_dt", "2012-10-18")
    u_dt = m_dt.try_uncollide_datetime(m_dt.expansion, "%Y-%m-%d")
    u_dtt = m_dt.try_uncollide()

    m_dt2 = MacroDef("test_dt", "9999-12-31")
    u_dt2 = m_d.try_uncollide_datetime(m_dt2.expansion, "%Y-%m-%d")

    m_s = MacroDef("test_dt", "toto")
    u_s = m_d.try_uncollide_string(m_s.expansion)

    m_s1 = MacroDef("test_dt", "'toto'")
    u_s1 = m_d.try_uncollide_string(m_s1.expansion)

    m_s2 = MacroDef("test_dt", "'toto', 'titi' ")
    u_s2 = m_d.try_uncollide_string(m_s2.expansion)
    u_s2s = m_s2.try_uncollide()

    pass

    # a = MacroDef("test_dt", "9999-12-31 23:59:59")
    # a.guess_macro_type()
    # assert a.macro_type == "datetime:%Y-%m-%d %H:%M:%S"

    # a = MacroDef("test_dt", "31/12/9999")
    # a.guess_macro_type()
    # assert a.macro_type == "datetime:%d/%m/%Y"

    # a = MacroDef("test_dt", "2000/01/01 00:00:00")
    # a.guess_macro_type()
    # assert a.macro_type == "datetime:%Y/%m/%d %H:%M:%S"

    # a = MacroDef("test_dt", "2000/01*01 80:80:80")
    # a.guess_macro_type()
    # assert a.macro_type == "string"
    # assert a.quote == False

    # a = MacroDef("test_dt", "'azerty'")
    # a.guess_macro_type()
    # assert a.macro_type == "string"
    # assert a.quote == True

    # a = MacroDef("test_dt", "COM_CAP_VM_MM2_CY2")
    # a.guess_macro_type()
    # assert a.macro_type == "database"
