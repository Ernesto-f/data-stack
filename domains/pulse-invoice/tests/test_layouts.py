from layouts import classify_nfse_layout


def test_classifies_current_municipal_layout_family():
    text = """
    PREFEITURA MUNICIPAL DE EXEMPLO
    NOTA FISCAL DE SERVIÇOS ELETRÔNICA
    Número da Nota: 12345
    PRESTADOR DE SERVIÇOS
    TOMADOR DE SERVIÇOS
    VALOR TOTAL DO SERVIÇO = R$ 123,45
    """
    assert classify_nfse_layout(text) == "municipal_classic_v1"


def test_marks_national_nfse_as_unmapped_until_profile_is_validated():
    text = """
    DANFSe - Documento Auxiliar da Nota Fiscal de Serviço Eletrônica Nacional
    NFS-e Nacional
    """
    assert classify_nfse_layout(text) == "nfse_nacional_unmapped"


def test_marks_emitente_destinatario_variant_as_national_unmapped():
    text = """
    Identificação do Emitente
    Identificação do Destinatário
    CPF/CNPJ
    Valor Total
    ISS retido
    """
    assert classify_nfse_layout(text) == "nfse_nacional_candidate_v1"


def test_unknown_format_fails_closed():
    assert classify_nfse_layout("Documento sem cabeçalhos fiscais reconhecidos") == "unclassified"
