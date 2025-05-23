from typing import List, Dict, Any, Tuple
from datetime import date, datetime, timedelta
from decimal import Decimal
import calendar
from collections import defaultdict

from models import OperacaoCreate, AtualizacaoCarteira
from database import (
    inserir_operacao,
    obter_todas_operacoes,
    atualizar_carteira,
    obter_carteira_atual,
    salvar_resultado_mensal,
    obter_resultados_mensais
)

def processar_operacoes(operacoes: List[OperacaoCreate]) -> None:
    """
    Processa uma lista de operações, salvando-as no banco de dados
    e atualizando a carteira atual.
    
    Args:
        operacoes: Lista de operações a serem processadas.
    """
    # Salva as operações no banco de dados
    for op in operacoes:
        inserir_operacao(op.dict())
    
    # Recalcula a carteira atual
    recalcular_carteira()
    
    # Recalcula os resultados mensais
    recalcular_resultados()

def _eh_day_trade(operacoes_dia: List[Dict[str, Any]], ticker: str) -> bool:
    """
    Verifica se houve day trade para um ticker específico em um dia.
    
    Args:
        operacoes_dia: Lista de operações do dia.
        ticker: Ticker a ser verificado.
        
    Returns:
        bool: True se houve day trade, False caso contrário.
    """
    compras = sum(op["quantity"] for op in operacoes_dia 
                 if op["ticker"] == ticker and op["operation"] == "buy")
    vendas = sum(op["quantity"] for op in operacoes_dia 
                if op["ticker"] == ticker and op["operation"] == "sell")
    
    # Se houve compra e venda do mesmo ticker no mesmo dia, é day trade
    return compras > 0 and vendas > 0

def _calcular_resultado_dia(operacoes_dia: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Calcula o resultado de swing trade e day trade para um dia.
    
    Args:
        operacoes_dia: Lista de operações do dia.
        
    Returns:
        Tuple[Dict[str, float], Dict[str, float]]: Resultados de swing trade e day trade.
    """
    # Dicionários para armazenar os resultados
    resultado_swing = {
        "vendas": 0.0,
        "custo": 0.0,
        "ganho_liquido": 0.0
    }
    
    resultado_day = {
        "vendas": 0.0,
        "custo": 0.0,
        "ganho_liquido": 0.0,
        "irrf": 0.0
    }
    
    # Identifica os tickers com day trade
    tickers_day_trade = set()
    for ticker in set(op["ticker"] for op in operacoes_dia):
        if _eh_day_trade(operacoes_dia, ticker):
            tickers_day_trade.add(ticker)
    
    # Processa as operações
    for op in operacoes_dia:
        ticker = op["ticker"]
        valor = op["quantity"] * op["price"]
        fees = op["fees"]
        
        # Verifica se é day trade ou swing trade
        if ticker in tickers_day_trade:
            # Day Trade
            if op["operation"] == "buy":
                resultado_day["custo"] += valor + fees
            else:  # sell
                resultado_day["vendas"] += valor - fees
                # IRRF de 1% sobre o valor da venda em day trade
                resultado_day["irrf"] += valor * 0.01
        else:
            # Swing Trade
            if op["operation"] == "buy":
                # Compras de swing trade não afetam o resultado do dia
                pass
            else:  # sell
                resultado_swing["vendas"] += valor
                
                # Para calcular o custo, precisamos do preço médio da carteira
                # Isso é uma simplificação, na prática precisaríamos rastrear o preço médio
                # de cada ticker ao longo do tempo
                carteira = obter_carteira_atual()
                ticker_info = next((item for item in carteira if item["ticker"] == ticker), None)
                
                if ticker_info:
                    preco_medio = ticker_info["preco_medio"]
                    custo = op["quantity"] * preco_medio
                    resultado_swing["custo"] += custo
                
                # IRRF de 0.005% sobre o valor da venda em swing trade
                # Não estamos calculando explicitamente, pois é muito pequeno
    
    # Calcula os ganhos líquidos
    resultado_swing["ganho_liquido"] = resultado_swing["vendas"] - resultado_swing["custo"]
    resultado_day["ganho_liquido"] = resultado_day["vendas"] - resultado_day["custo"]
    
    return resultado_swing, resultado_day

def calcular_resultados_mensais() -> List[Dict[str, Any]]:
    """
    Obtém os resultados mensais calculados.
    
    Returns:
        List[Dict[str, Any]]: Lista de resultados mensais.
    """
    return obter_resultados_mensais()

def calcular_carteira_atual() -> List[Dict[str, Any]]:
    """
    Obtém a carteira atual de ações.
    
    Returns:
        List[Dict[str, Any]]: Lista de itens da carteira.
    """
    return obter_carteira_atual()

def gerar_darfs() -> List[Dict[str, Any]]:
    """
    Gera a lista de DARFs a partir dos resultados mensais.
    
    Returns:
        List[Dict[str, Any]]: Lista de DARFs.
    """
    resultados = obter_resultados_mensais()
    
    darfs = []
    for resultado in resultados:
        if resultado.get("darf_codigo") and resultado.get("darf_valor", 0) > 0:
            darfs.append({
                "codigo": resultado["darf_codigo"],
                "competencia": resultado["darf_competencia"],
                "valor": resultado["darf_valor"],
                "vencimento": resultado["darf_vencimento"]
            })
    
    return darfs

# Novas funções para as funcionalidades adicionais

def inserir_operacao_manual(operacao: OperacaoCreate) -> None:
    """
    Insere uma operação manualmente e recalcula a carteira e os resultados.
    
    Args:
        operacao: Dados da operação a ser inserida.
    """
    # Insere a operação no banco de dados
    inserir_operacao(operacao.dict())
    
    # Recalcula a carteira e os resultados
    recalcular_carteira()
    recalcular_resultados()

def atualizar_item_carteira(dados: AtualizacaoCarteira) -> None:
    """
    Atualiza um item da carteira manualmente.
    
    Args:
        dados: Novos dados do item da carteira (ticker, quantidade e preço médio).
    """
    # Atualiza o item na carteira
    atualizar_carteira(dados.ticker, dados.quantidade, dados.preco_medio)


            
            

def calcular_operacoes_fechadas() -> List[Dict[str, Any]]:
    """
    Calcula as operações fechadas (compra seguida de venda ou vice-versa).
    Usa o método FIFO (First In, First Out) para rastrear as operações.
    
    Returns:
        List[Dict[str, Any]]: Lista de operações fechadas.
    """
    # Obtém todas as operações
    operacoes = obter_todas_operacoes()
    
    # Dicionário para rastrear as operações por ticker
    operacoes_por_ticker = defaultdict(list)
    for op in operacoes:
        operacoes_por_ticker[op["ticker"]].append(op)
    
    # Lista para armazenar as operações fechadas
    operacoes_fechadas = []
    
    # Processa cada ticker
    for ticker, ops in operacoes_por_ticker.items():
        # Ordena as operações por data
        ops.sort(key=lambda x: (x["date"], x["id"]))
        
        # Filas para rastrear compras e vendas pendentes
        compras_pendentes = []
        vendas_pendentes = []
        
        # Processa cada operação
        for op in ops:
            if op["operation"] == "buy":
                # Se há vendas pendentes, fecha as operações
                if vendas_pendentes:
                    _processar_fechamento_operacoes(op, vendas_pendentes, operacoes_fechadas, "venda-compra")
                else:
                    # Adiciona a compra à fila de pendentes
                    compras_pendentes.append(op)
            else:  # sell
                # Se há compras pendentes, fecha as operações
                if compras_pendentes:
                    _processar_fechamento_operacoes(op, compras_pendentes, operacoes_fechadas, "compra-venda")
                else:
                    # Adiciona a venda à fila de pendentes (venda a descoberto)
                    vendas_pendentes.append(op)
    
    return operacoes_fechadas

def _processar_fechamento_operacoes(op_atual, ops_pendentes, operacoes_fechadas, tipo):
    """
    Processa o fechamento de operações usando o método FIFO.
    
    Args:
        op_atual: Operação atual.
        ops_pendentes: Lista de operações pendentes.
        operacoes_fechadas: Lista de operações fechadas.
        tipo: Tipo de fechamento ("compra-venda" ou "venda-compra").
    """
    quantidade_restante = op_atual["quantity"]
    
    while quantidade_restante > 0 and ops_pendentes:
        # Obtém a operação pendente mais antiga (FIFO)
        op_pendente = ops_pendentes[0]
        
        # Determina a quantidade a ser fechada
        quantidade_fechada = min(quantidade_restante, op_pendente["quantity"])
        
        # Calcula o resultado
        if tipo == "compra-venda":
            # Compra seguida de venda
            preco_compra = op_pendente["price"]
            preco_venda = op_atual["price"]
            taxas_compra = op_pendente["fees"] * (quantidade_fechada / op_pendente["quantity"])
            taxas_venda = op_atual["fees"] * (quantidade_fechada / op_atual["quantity"])
        else:
            # Venda seguida de compra (venda a descoberto)
            preco_venda = op_pendente["price"]
            preco_compra = op_atual["price"]
            taxas_venda = op_pendente["fees"] * (quantidade_fechada / op_pendente["quantity"])
            taxas_compra = op_atual["fees"] * (quantidade_fechada / op_atual["quantity"])
        
        valor_compra = preco_compra * quantidade_fechada
        valor_venda = preco_venda * quantidade_fechada
        taxas_total = taxas_compra + taxas_venda
        resultado = valor_venda - valor_compra - taxas_total
        
        # Verifica se é day trade
        day_trade = op_pendente["date"] == op_atual["date"]
        
        # Cria os detalhes das operações
        if tipo == "compra-venda":
            op_abertura = {
                "id": op_pendente["id"],
                "date": op_pendente["date"],
                "operation": "buy",
                "quantity": quantidade_fechada,
                "price": preco_compra,
                "fees": taxas_compra,
                "valor_total": valor_compra + taxas_compra
            }
            op_fechamento = {
                "id": op_atual["id"],
                "date": op_atual["date"],
                "operation": "sell",
                "quantity": quantidade_fechada,
                "price": preco_venda,
                "fees": taxas_venda,
                "valor_total": valor_venda - taxas_venda
            }
            data_abertura = op_pendente["date"]
            data_fechamento = op_atual["date"]
        else:
            op_abertura = {
                "id": op_pendente["id"],
                "date": op_pendente["date"],
                "operation": "sell",
                "quantity": quantidade_fechada,
                "price": preco_venda,
                "fees": taxas_venda,
                "valor_total": valor_venda - taxas_venda
            }
            op_fechamento = {
                "id": op_atual["id"],
                "date": op_atual["date"],
                "operation": "buy",
                "quantity": quantidade_fechada,
                "price": preco_compra,
                "fees": taxas_compra,
                "valor_total": valor_compra + taxas_compra
            }
            data_abertura = op_pendente["date"]
            data_fechamento = op_atual["date"]
        
        # Cria a operação fechada
        operacao_fechada = {
            "ticker": op_atual["ticker"],
            "data_abertura": data_abertura,
            "data_fechamento": data_fechamento,
            "tipo": tipo,
            "quantidade": quantidade_fechada,
            "preco_abertura": preco_compra if tipo == "compra-venda" else preco_venda,
            "preco_fechamento": preco_venda if tipo == "compra-venda" else preco_compra,
            "taxas_total": taxas_total,
            "resultado": resultado,
            "operacoes_relacionadas": [op_abertura, op_fechamento],
            "day_trade": day_trade
        }
        
        # Adiciona a operação fechada à lista
        operacoes_fechadas.append(operacao_fechada)
        
        # Atualiza a quantidade restante
        quantidade_restante -= quantidade_fechada
        
        # Atualiza a operação pendente
        op_pendente["quantity"] -= quantidade_fechada
        if op_pendente["quantity"] == 0:
            # Remove a operação pendente se foi totalmente fechada
            ops_pendentes.pop(0)
        
    # Se ainda há quantidade restante, adiciona à fila de pendentes
    if quantidade_restante > 0:
        op_restante = op_atual.copy()
        op_restante["quantity"] = quantidade_restante
        if tipo == "compra-venda":
            # Adiciona a compra restante à fila de pendentes
            ops_pendentes.append(op_restante)
        else:
            # Adiciona a venda restante à fila de pendentes
            ops_pendentes.append(op_restante)

def recalcular_carteira() -> None:
    """
    Recalcula a carteira atual com base em todas as operações.
    """
    # Obtém todas as operações
    operacoes = obter_todas_operacoes()  # Corrigido de obter_todas_operações
    
    # Dicionário para armazenar a carteira atual
    carteira = defaultdict(lambda: {"quantidade": 0, "custo_total": 0.0})
    
    # Processa cada operação
    for op in operacoes:
        ticker = op["ticker"]
        quantidade = op["quantity"]
        valor = quantidade * op["price"]
        
        if op["operation"] == "buy":
            # Compra: adiciona à carteira
            carteira[ticker]["quantidade"] += quantidade
            carteira[ticker]["custo_total"] += valor + op["fees"]
        else:
            # Venda: remove da carteira
            # Calcula o preço médio atual
            preco_medio = (carteira[ticker]["custo_total"] / carteira[ticker]["quantidade"] 
                          if carteira[ticker]["quantidade"] > 0 else 0)
            
            # Atualiza a quantidade
            carteira[ticker]["quantidade"] -= quantidade
            
            # Atualiza o custo total proporcionalmente
            if carteira[ticker]["quantidade"] > 0:
                carteira[ticker]["custo_total"] = preco_medio * carteira[ticker]["quantidade"]
            else:
                carteira[ticker]["custo_total"] = 0
    
    # Atualiza a carteira no banco de dados
    for ticker, dados in carteira.items():
        if dados["quantidade"] != 0:  # Só mantém na carteira se ainda tiver ações
            atualizar_carteira(ticker, dados["quantidade"], dados["custo_total"] / dados["quantidade"])

def recalcular_resultados() -> None:
    """
    Recalcula os resultados mensais com base em todas as operações.
    """
    # Obtém todas as operações
    operacoes = obter_todas_operacoes()
    
    # Agrupa as operações por mês
    operacoes_por_mes = defaultdict(list)
    for op in operacoes:
        mes = op["date"].strftime("%Y-%m")
        operacoes_por_mes[mes].append(op)
    
    # Dicionários para armazenar os prejuízos acumulados
    prejuizo_acumulado_swing = 0.0
    prejuizo_acumulado_day = 0.0
    
    # Processa cada mês
    for mes, ops_mes in sorted(operacoes_por_mes.items()):
        # Agrupa as operações por dia
        operacoes_por_dia = defaultdict(list)
        for op in ops_mes:
            dia = op["date"].isoformat()
            operacoes_por_dia[dia].append(op)
        
        # Inicializa os resultados do mês
        resultado_mes_swing = {
            "vendas": 0.0,
            "custo": 0.0,
            "ganho_liquido": 0.0
        }
        
        resultado_mes_day = {
            "vendas": 0.0,
            "custo": 0.0,
            "ganho_liquido": 0.0,
            "irrf": 0.0
        }
        
        # Processa cada dia
        for dia, ops_dia in sorted(operacoes_por_dia.items()):
            resultado_dia_swing, resultado_dia_day = _calcular_resultado_dia(ops_dia)
            
            # Acumula os resultados do dia no mês
            resultado_mes_swing["vendas"] += resultado_dia_swing["vendas"]
            resultado_mes_swing["custo"] += resultado_dia_swing["custo"]
            resultado_mes_swing["ganho_liquido"] += resultado_dia_swing["ganho_liquido"]
            
            resultado_mes_day["vendas"] += resultado_dia_day["vendas"]
            resultado_mes_day["custo"] += resultado_dia_day["custo"]
            resultado_mes_day["ganho_liquido"] += resultado_dia_day["ganho_liquido"]
            resultado_mes_day["irrf"] += resultado_dia_day["irrf"]
        
        # Verifica se o swing trade é isento (vendas mensais até R$ 20.000)
        isento_swing = resultado_mes_swing["vendas"] <= 20000.0
        
        # Aplica a compensação de prejuízos
        if prejuizo_acumulado_swing > 0 and resultado_mes_swing["ganho_liquido"] > 0:
            # Compensa o prejuízo acumulado de swing trade
            compensacao = min(prejuizo_acumulado_swing, resultado_mes_swing["ganho_liquido"])
            resultado_mes_swing["ganho_liquido"] -= compensacao
            prejuizo_acumulado_swing -= compensacao
        elif resultado_mes_swing["ganho_liquido"] < 0:
            # Acumula o prejuízo de swing trade
            prejuizo_acumulado_swing += abs(resultado_mes_swing["ganho_liquido"])
            resultado_mes_swing["ganho_liquido"] = 0
        
        if prejuizo_acumulado_day > 0 and resultado_mes_day["ganho_liquido"] > 0:
            # Compensa o prejuízo acumulado de day trade
            compensacao = min(prejuizo_acumulado_day, resultado_mes_day["ganho_liquido"])
            resultado_mes_day["ganho_liquido"] -= compensacao
            prejuizo_acumulado_day -= compensacao
        elif resultado_mes_day["ganho_liquido"] < 0:
            # Acumula o prejuízo de day trade
            prejuizo_acumulado_day += abs(resultado_mes_day["ganho_liquido"])
            resultado_mes_day["ganho_liquido"] = 0
        
        # Calcula o IR devido
        ir_devido_swing = 0.0 if isento_swing else resultado_mes_swing["ganho_liquido"] * 0.15
        ir_devido_day = max(0, resultado_mes_day["ganho_liquido"] * 0.20)
        
        # Calcula o IR a pagar (já descontando o IRRF)
        ir_pagar_swing = max(0, ir_devido_swing - (resultado_mes_swing["vendas"] * 0.00005))
        ir_pagar_day = max(0, ir_devido_day - resultado_mes_day["irrf"])
        
        # Gera o DARF se necessário
        darf = None
        if ir_pagar_day > 0:
            # Calcula a data de vencimento (último dia útil do mês seguinte)
            ano, mes_num = map(int, mes.split('-'))
            ultimo_dia = calendar.monthrange(ano, mes_num + 1 if mes_num < 12 else 1)[1]
            vencimento = date(ano if mes_num < 12 else ano + 1, mes_num + 1 if mes_num < 12 else 1, ultimo_dia)
            
            # Ajusta para o último dia útil (simplificação: considera apenas finais de semana)
            while vencimento.weekday() >= 5:  # 5 = sábado, 6 = domingo
                vencimento -= timedelta(days=1)
            
            darf = {
                "codigo": "6015",
                "competencia": mes,
                "valor": ir_pagar_day,
                "vencimento": vencimento
            }
        
        # Salva o resultado mensal
        resultado = {
            "mes": mes,
            "vendas_swing": resultado_mes_swing["vendas"],
            "custo_swing": resultado_mes_swing["custo"],
            "ganho_liquido_swing": resultado_mes_swing["ganho_liquido"],
            "isento_swing": isento_swing,
            "ganho_liquido_day": resultado_mes_day["ganho_liquido"],
            "ir_devido_day": ir_devido_day,
            "irrf_day": resultado_mes_day["irrf"],
            "ir_pagar_day": ir_pagar_day,
            "prejuizo_acumulado_swing": prejuizo_acumulado_swing,
            "prejuizo_acumulado_day": prejuizo_acumulado_day
        }
        
        if darf:
            resultado.update({
                "darf_codigo": darf["codigo"],
                "darf_competencia": darf["competencia"],
                "darf_valor": darf["valor"],
                "darf_vencimento": darf["vencimento"]
            })
        
        # Salva o resultado mensal no banco de dados
        salvar_resultado_mensal(resultado)
    """
    Recalcula os resultados mensais com base em todas as operações.
    """
    # Obtém todas as operações
    operacoes = obter_todas_operacoes()
    
    # Agrupa as operações por mês
    operacoes_por_mes = defaultdict(list)
    for op in operacoes:
        mes = op["date"].strftime("%Y-%m")
        operacoes_por_mes[mes].append(op)
    
    # Dicionários para armazenar os prejuízos acumulados
    prejuizo_acumulado_swing = 0.0
    prejuizo_acumulado_day = 0.0
    
    # Processa cada mês
    for mes, ops_mes in sorted(operacoes_por_mes.items()):
        # Agrupa as operações por dia
        operacoes_por_dia = defaultdict(list)
        for op in ops_mes:
            dia = op["date"].isoformat()
            operacoes_por_dia[dia].append(op)
        
        # Inicializa os resultados do mês
        resultado_mes_swing = {
            "vendas": 0.0,
            "custo": 0.0,
            "ganho_liquido": 0.0
        }
        
        resultado_mes_day = {
            "vendas": 0.0,
            "custo": 0.0,
            "ganho_liquido": 0.0,
            "irrf": 0.0
        }
        
        # Processa cada dia
        for dia, ops_dia in sorted(operacoes_por_dia.items()):
            resultado_dia_swing, resultado_dia_day = _calcular_resultado_dia(ops_dia)
            
            # Acumula os resultados do dia no mês
            resultado_mes_swing["vendas"] += resultado_dia_swing["vendas"]
            resultado_mes_swing["custo"] += resultado_dia_swing["custo"]
            resultado_mes_swing["ganho_liquido"] += resultado_dia_swing["ganho_liquido"]
            
            resultado_mes_day["vendas"] += resultado_dia_day["vendas"]
            resultado_mes_day["custo"] += resultado_dia_day["custo"]
            resultado_mes_day["ganho_liquido"] += resultado_dia_day["ganho_liquido"]
            resultado_mes_day["irrf"] += resultado_dia_day["irrf"]
        
        # Verifica se o swing trade é isento (vendas mensais até R$ 20.000)
        isento_swing = resultado_mes_swing["vendas"] <= 20000.0
        
        # Aplica a compensação de prejuízos
        if prejuizo_acumulado_swing > 0 and resultado_mes_swing["ganho_liquido"] > 0:
            # Compensa o prejuízo acumulado de swing trade
            compensacao = min(prejuizo_acumulado_swing, resultado_mes_swing["ganho_liquido"])
            resultado_mes_swing["ganho_liquido"] -= compensacao
            prejuizo_acumulado_swing -= compensacao
        elif resultado_mes_swing["ganho_liquido"] < 0:
            # Acumula o prejuízo de swing trade
            prejuizo_acumulado_swing += abs(resultado_mes_swing["ganho_liquido"])
            resultado_mes_swing["ganho_liquido"] = 0
        
        if prejuizo_acumulado_day > 0 and resultado_mes_day["ganho_liquido"] > 0:
            # Compensa o prejuízo acumulado de day trade
            compensacao = min(prejuizo_acumulado_day, resultado_mes_day["ganho_liquido"])
            resultado_mes_day["ganho_liquido"] -= compensacao
            prejuizo_acumulado_day -= compensacao
        elif resultado_mes_day["ganho_liquido"] < 0:
            # Acumula o prejuízo de day trade
            prejuizo_acumulado_day += abs(resultado_mes_day["ganho_liquido"])
            resultado_mes_day["ganho_liquido"] = 0
        
        # Calcula o IR devido
        ir_devido_swing = 0.0 if isento_swing else resultado_mes_swing["ganho_liquido"] * 0.15
        ir_devido_day = max(0, resultado_mes_day["ganho_liquido"] * 0.20)
        
        # Calcula o IR a pagar (já descontando o IRRF)
        ir_pagar_swing = max(0, ir_devido_swing - (resultado_mes_swing["vendas"] * 0.00005))
        ir_pagar_day = max(0, ir_devido_day - resultado_mes_day["irrf"])
        
        # Gera o DARF se necessário
        darf = None
        if ir_pagar_day > 0:
            # Calcula a data de vencimento (último dia útil do mês seguinte)
            ano, mes_num = map(int, mes.split('-'))
            ultimo_dia = calendar.monthrange(ano, mes_num + 1 if mes_num < 12 else 1)[1]
            vencimento = date(ano if mes_num < 12 else ano + 1, mes_num + 1 if mes_num < 12 else 1, ultimo_dia)
            
            # Ajusta para o último dia útil (simplificação: considera apenas finais de semana)
            while vencimento.weekday() >= 5:  # 5 = sábado, 6 = domingo
                vencimento -= timedelta(days=1)
            
            darf = {
                "codigo": "6015",
                "competencia": mes,
                "valor": ir_pagar_day,
                "vencimento": vencimento
            }
        
        # Salva o resultado mensal
        resultado = {
            "mes": mes,
            "vendas_swing": resultado_mes_swing["vendas"],
            "custo_swing": resultado_mes_swing["custo"],
            "ganho_liquido_swing": resultado_mes_swing["ganho_liquido"],
            "isento_swing": isento_swing,
            "ganho_liquido_day": resultado_mes_day["ganho_liquido"],
            "ir_devido_day": ir_devido_day,
            "irrf_day": resultado_mes_day["irrf"],
            "ir_pagar_day": ir_pagar_day,
            "prejuizo_acumulado_swing": prejuizo_acumulado_swing,
            "prejuizo_acumulado_day": prejuizo_acumulado_day
        }
        
        if darf:
            resultado.update({
                "darf_codigo": darf["codigo"],
                "darf_competencia": darf["competencia"],
                "darf_valor": darf["valor"],
                "darf_vencimento": darf["vencimento"]
            })
        
        # ERRO AQUI: A função está sendo chamada com dois argumentos, mas espera apenas um
        # Código incorreto: salvar_resultado_mensal(data_primeiro_dia, dados["resultado"])
        # Código correto:
        salvar_resultado_mensal(resultado)
    """
    Recalcula os resultados mensais com base em todas as operações.
    """
    # Obtém todas as operações
    operacoes = obter_todas_operacoes()  # Corrigido de obter_todas_operações
    
    # Dicionário para armazenar resultados mensais
    resultados_mensais = defaultdict(lambda: {"resultado": 0.0})
    
    # Processa cada operação
    for op in operacoes:
        ticker = op["ticker"]
        quantidade = op["quantity"]
        valor = quantidade * op["price"]
        data = op["date"]
        
        # Formata a data para o início do mês
        ano, mes = data.year, data.month
        mes_primeiro_dia = date(ano, mes, 1)
        
        if op["operation"] == "buy":
            # Compra: subtrai do resultado mensal
            resultados_mensais[mes_primeiro_dia]["resultado"] -= valor + op["fees"]
        else:
            # Venda: adiciona ao resultado mensal
            resultados_mensais[mes_primeiro_dia]["resultado"] += valor - op["fees"]
    
    # Salva os resultados mensais no banco de dados
    for data_primeiro_dia, dados in resultados_mensais.items():
        salvar_resultado_mensal(data_primeiro_dia, dados["resultado"])            