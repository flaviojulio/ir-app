import pytest
from datetime import date
import os

from models import OperacaoCreate
from services import (
    processar_operacoes,
    calcular_resultados_mensais,
    calcular_carteira_atual,
    gerar_darfs
)
from database import criar_tabelas

# Configuração para usar um banco de dados de teste
@pytest.fixture(autouse=True)
def setup_database():
    """
    Configura um banco de dados de teste para os testes.
    """
    # Altera o caminho do banco de dados para um arquivo de teste
    import database
    database.DATABASE_PATH = "test_acoes_ir.db"
    
    # Cria as tabelas
    criar_tabelas()
    
    yield
    
    # Remove o banco de dados de teste após os testes
    try:
        os.remove("test_acoes_ir.db")
    except:
        pass

def test_processar_operacoes():
    """
    Testa o processamento de operações.
    """
    # Cria algumas operações de teste
    operacoes = [
        OperacaoCreate(
            date=date(2025, 1, 10),
            ticker="PETR4",
            operation="buy",
            quantity=100,
            price=28.50,
            fees=5.20
        ),
        OperacaoCreate(
            date=date(2025, 1, 15),
            ticker="PETR4",
            operation="sell",
            quantity=50,
            price=30.00,
            fees=3.10
        )
    ]
    
    # Processa as operações
    processar_operacoes(operacoes)
    
    # Verifica se a carteira foi atualizada corretamente
    carteira = calcular_carteira_atual()
    assert len(carteira) == 1
    assert carteira[0]["ticker"] == "PETR4"
    assert carteira[0]["quantidade"] == 50
    
    # Verifica se os resultados mensais foram calculados
    resultados = calcular_resultados_mensais()
    assert len(resultados) == 1
    assert resultados[0]["mes"] == "2025-01"
    assert resultados[0]["isento_swing"] == True  # Vendas abaixo de R$ 20.000

def test_day_trade():
    """
    Testa o cálculo de day trade.
    """
    # Cria operações de day trade
    operacoes = [
        OperacaoCreate(
            date=date(2025, 2, 10),
            ticker="ITUB4",
            operation="buy",
            quantity=100,
            price=30.00,
            fees=5.00
        ),
        OperacaoCreate(
            date=date(2025, 2, 10),
            ticker="ITUB4",
            operation="sell",
            quantity=100,
            price=32.00,
            fees=5.00
        )
    ]
    
    # Processa as operações
    processar_operacoes(operacoes)
    
    # Verifica se os resultados mensais foram calculados corretamente
    resultados = calcular_resultados_mensais()
    resultado_fev = next((r for r in resultados if r["mes"] == "2025-02"), None)
    
    assert resultado_fev is not None
    assert resultado_fev["ganho_liquido_day"] > 0  # Deve ter lucro em day trade
    assert resultado_fev["ir_devido_day"] > 0  # Deve ter IR devido em day trade

def test_swing_trade_isento():
    """
    Testa o cálculo de swing trade isento.
    """
    # Cria operações de swing trade com vendas abaixo de R$ 20.000
    operacoes = [
        OperacaoCreate(
            date=date(2025, 3, 5),
            ticker="VALE3",
            operation="buy",
            quantity=100,
            price=80.00,
            fees=10.00
        ),
        OperacaoCreate(
            date=date(2025, 3, 15),
            ticker="VALE3",
            operation="sell",
            quantity=50,
            price=85.00,
            fees=5.00
        )
    ]
    
    # Processa as operações
    processar_operacoes(operacoes)
    
    # Verifica se os resultados mensais foram calculados corretamente
    resultados = calcular_resultados_mensais()
    resultado_mar = next((r for r in resultados if r["mes"] == "2025-03"), None)
    
    assert resultado_mar is not None
    assert resultado_mar["vendas_swing"] < 20000  # Vendas abaixo de R$ 20.000
    assert resultado_mar["isento_swing"] == True  # Deve ser isento

def test_swing_trade_tributavel():
    """
    Testa o cálculo de swing trade tributável.
    """
    # Cria operações de swing trade com vendas acima de R$ 20.000
    operacoes = [
        OperacaoCreate(
            date=date(2025, 4, 5),
            ticker="BBAS3",
            operation="buy",
            quantity=1000,
            price=30.00,
            fees=50.00
        ),
        OperacaoCreate(
            date=date(2025, 4, 20),
            ticker="BBAS3",
            operation="sell",
            quantity=800,
            price=32.00,
            fees=40.00
        )
    ]
    
    # Processa as operações
    processar_operacoes(operacoes)
    
    # Verifica se os resultados mensais foram calculados corretamente
    resultados = calcular_resultados_mensais()
    resultado_abr = next((r for r in resultados if r["mes"] == "2025-04"), None)
    
    assert resultado_abr is not None
    assert resultado_abr["vendas_swing"] > 20000  # Vendas acima de R$ 20.000
    assert resultado_abr["isento_swing"] == False  # Não deve ser isento

def test_compensacao_prejuizo():
    """
    Testa a compensação de prejuízos.
    """
    # Cria operações com prejuízo
    operacoes_prejuizo = [
        OperacaoCreate(
            date=date(2025, 5, 5),
            ticker="MGLU3",
            operation="buy",
            quantity=1000,
            price=10.00,
            fees=20.00
        ),
        OperacaoCreate(
            date=date(2025, 5, 20),
            ticker="MGLU3",
            operation="sell",
            quantity=1000,
            price=8.00,
            fees=20.00
        )
    ]
    
    # Processa as operações com prejuízo
    processar_operacoes(operacoes_prejuizo)
    
    # Verifica se o prejuízo foi registrado
    resultados = calcular_resultados_mensais()
    resultado_mai = next((r for r in resultados if r["mes"] == "2025-05"), None)
    
    assert resultado_mai is not None
    assert resultado_mai["prejuizo_acumulado_swing"] > 0  # Deve ter prejuízo acumulado
    
    # Cria operações com lucro no mês seguinte
    operacoes_lucro = [
        OperacaoCreate(
            date=date(2025, 6, 5),
            ticker="WEGE3",
            operation="buy",
            quantity=500,
            price=40.00,
            fees=30.00
        ),
        OperacaoCreate(
            date=date(2025, 6, 20),
            ticker="WEGE3",
            operation="sell",
            quantity=500,
            price=45.00,
            fees=30.00
        )
    ]
    
    # Processa as operações com lucro
    processar_operacoes(operacoes_lucro)
    
    # Verifica se o prejuízo foi compensado
    resultados = calcular_resultados_mensais()
    resultado_jun = next((r for r in resultados if r["mes"] == "2025-06"), None)
    
    assert resultado_jun is not None
    assert resultado_jun["prejuizo_acumulado_swing"] < resultado_mai["prejuizo_acumulado_swing"]  # Prejuízo deve ter diminuído

def test_gerar_darfs():
    """
    Testa a geração de DARFs.
    """
    # Cria operações de day trade com lucro significativo
    operacoes = [
        OperacaoCreate(
            date=date(2025, 7, 10),
            ticker="PETR4",
            operation="buy",
            quantity=1000,
            price=30.00,
            fees=50.00
        ),
        OperacaoCreate(
            date=date(2025, 7, 10),
            ticker="PETR4",
            operation="sell",
            quantity=1000,
            price=35.00,
            fees=50.00
        )
    ]
    
    # Processa as operações
    processar_operacoes(operacoes)
    
    # Verifica se os DARFs foram gerados
    darfs = gerar_darfs()
    
    assert len(darfs) > 0
    assert darfs[0]["codigo"] == "6015"
    assert darfs[0]["competencia"] == "2025-07"
    assert darfs[0]["valor"] > 0