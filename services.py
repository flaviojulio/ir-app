from typing import List, Dict, Any, Optional # Tuple replaced with tuple, Optional added
from datetime import date, datetime, timedelta
from decimal import Decimal # Kept for specific calculations in recalcular_resultados
import calendar
from collections import defaultdict

from models import OperacaoCreate, AtualizacaoCarteira, Operacao # Operacao added
from database import (
    inserir_operacao,
    obter_todas_operacoes, # Comment removed
    atualizar_carteira,
    obter_carteira_atual,
    salvar_resultado_mensal,
    obter_resultados_mensais,
    obter_operacao_por_id, # Added
    # Import new/updated database functions
    obter_operacoes_para_calculo_fechadas,
    salvar_operacao_fechada,
    limpar_operacoes_fechadas_usuario,
    remover_operacao  # Added import for remover_operacao
)

def processar_operacoes(operacoes: List[OperacaoCreate], usuario_id: int) -> None:
    """
    Processa uma lista de operações, salvando-as no banco de dados
    e atualizando a carteira atual para um usuário específico.
    
    Args:
        operacoes: Lista de operações a serem processadas.
        usuario_id: ID do usuário.
    """
    # Salva as operações no banco de dados
    for op in operacoes:
        inserir_operacao(op.model_dump(), usuario_id=usuario_id) # Use model_dump() for Pydantic v2
    
    # Recalcula a carteira atual
    recalcular_carteira(usuario_id=usuario_id)
    
    # Recalcula os resultados mensais
    recalcular_resultados(usuario_id=usuario_id)

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

def _calcular_resultado_dia(operacoes_dia: List[Dict[str, Any]], usuario_id: int) -> tuple[Dict[str, float], Dict[str, float]]: # Changed Tuple to tuple
    """
    Calcula o resultado de swing trade e day trade para um dia para um usuário.
    
    Args:
        operacoes_dia: Lista de operações do dia.
        usuario_id: ID do usuário.
        
    Returns:
        tuple[Dict[str, float], Dict[str, float]]: Resultados de swing trade e day trade.
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
        fees = op.get("fees", 0.0) # Garante que fees tenha um valor padrão
        
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
                # Compras de swing trade não afetam o resultado do dia diretamente,
                # mas são usadas para calcular o preço médio na venda.
                pass
            else:  # sell
                resultado_swing["vendas"] += valor - fees # Subtrai taxas da venda também
                
                # Para calcular o custo, precisamos do preço médio da carteira do usuário
                carteira = obter_carteira_atual(usuario_id=usuario_id)
                ticker_info = next((item for item in carteira if item["ticker"] == ticker), None)
                
                if ticker_info:
                    preco_medio = ticker_info["preco_medio"]
                    custo = op["quantity"] * preco_medio
                    resultado_swing["custo"] += custo
                # Se não houver ticker_info, significa que está vendendo algo que não está na carteira
                # (pode ser venda a descoberto ou dados inconsistentes).
                # Para simplificar, não adicionamos custo se não estiver na carteira.
                
    # Calcula os ganhos líquidos
    resultado_swing["ganho_liquido"] = resultado_swing["vendas"] - resultado_swing["custo"]
    resultado_day["ganho_liquido"] = resultado_day["vendas"] - resultado_day["custo"]
    
    return resultado_swing, resultado_day

def calcular_resultados_mensais(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém os resultados mensais calculados para um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de resultados mensais.
    """
    return obter_resultados_mensais(usuario_id=usuario_id)

def calcular_carteira_atual(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém a carteira atual de ações de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de itens da carteira.
    """
    return obter_carteira_atual(usuario_id=usuario_id)

def gerar_darfs(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Gera a lista de DARFs a partir dos resultados mensais de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de DARFs.
    """
    resultados = obter_resultados_mensais(usuario_id=usuario_id)
    
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

def inserir_operacao_manual(operacao: OperacaoCreate, usuario_id: int) -> int:
    """
    Insere uma operação manualmente para um usuário e recalcula a carteira e os resultados.
    Retorna o ID da operação inserida.
    
    Args:
        operacao: Dados da operação a ser inserida.
        usuario_id: ID do usuário.
        
    Returns:
        int: ID da operação inserida.
    """
    # Insere a operação no banco de dados
    new_operacao_id = inserir_operacao(operacao.model_dump(), usuario_id=usuario_id)
    
    # Recalcula a carteira e os resultados
    recalcular_carteira(usuario_id=usuario_id)
    recalcular_resultados(usuario_id=usuario_id)
    return new_operacao_id

def obter_operacao_service(operacao_id: int, usuario_id: int) -> Optional[Operacao]:
    """
    Obtém uma operação específica pelo ID e ID do usuário.
    
    Args:
        operacao_id: ID da operação.
        usuario_id: ID do usuário.
        
    Returns:
        Optional[Operacao]: O objeto Operacao se encontrado, None caso contrário.
    """
    operacao_data = obter_operacao_por_id(operacao_id, usuario_id)
    if operacao_data:
        return Operacao(**operacao_data)
    return None

def atualizar_item_carteira(dados: AtualizacaoCarteira, usuario_id: int) -> None:
    """
    Atualiza um item da carteira manualmente para um usuário.
    
    Args:
        dados: Novos dados do item da carteira (ticker, quantidade e preço médio).
        usuario_id: ID do usuário.
    """
    # Atualiza o item na carteira
    atualizar_carteira(dados.ticker, dados.quantidade, dados.preco_medio, usuario_id=usuario_id)
    
    # Adiciona chamadas para recalcular tudo após a atualização manual da carteira
    recalcular_carteira(usuario_id=usuario_id)
    recalcular_resultados(usuario_id=usuario_id)
    calcular_operacoes_fechadas(usuario_id=usuario_id)


def calcular_operacoes_fechadas(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Calcula as operações fechadas para um usuário.
    Usa o método FIFO (First In, First Out) para rastrear as operações.
    Os resultados são salvos no banco de dados.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de operações fechadas.
    """
    # Limpa operações fechadas antigas do usuário
    limpar_operacoes_fechadas_usuario(usuario_id=usuario_id)

    # Obtém todas as operações do usuário
    operacoes = obter_operacoes_para_calculo_fechadas(usuario_id=usuario_id)
    
    # Dicionário para rastrear as operações por ticker
    operacoes_por_ticker = defaultdict(list)
    for op in operacoes:
        # Garante que 'fees' exista
        op_data = op.copy()
        if 'fees' not in op_data:
            op_data['fees'] = 0.0
        operacoes_por_ticker[op_data["ticker"]].append(op_data)
    
    # Lista para armazenar as operações fechadas que serão salvas
    operacoes_fechadas_para_salvar = []
    
    # Processa cada ticker
    for ticker, ops_ticker in operacoes_por_ticker.items():
        # Ordena as operações por data e depois por ID (para manter a ordem de inserção no mesmo dia)
        ops_ticker.sort(key=lambda x: (x["date"], x["id"]))
        
        # Filas para rastrear compras e vendas pendentes (FIFO)
        compras_pendentes = [] # Lista de Dicts de operações de compra
        vendas_pendentes = []  # Lista de Dicts de operações de venda (para venda a descoberto)
        
        for op_atual in ops_ticker:
            quantidade_atual = op_atual["quantity"]
            
            if op_atual["operation"] == "buy":
                # Tenta fechar com vendas pendentes (venda a descoberto)
                while quantidade_atual > 0 and vendas_pendentes:
                    venda_pendente = vendas_pendentes[0]
                    qtd_fechar = min(quantidade_atual, venda_pendente["quantity"])
                    
                    op_fechada = _criar_operacao_fechada_detalhada(
                        op_abertura=venda_pendente, 
                        op_fechamento=op_atual, 
                        quantidade_fechada=qtd_fechar,
                        tipo_fechamento="venda_descoberta_fechada_com_compra"
                    )
                    operacoes_fechadas_para_salvar.append(op_fechada)
                    
                    venda_pendente["quantity"] -= qtd_fechar
                    quantidade_atual -= qtd_fechar
                    
                    if venda_pendente["quantity"] == 0:
                        vendas_pendentes.pop(0)
                
                if quantidade_atual > 0:
                    op_atual_restante = op_atual.copy()
                    op_atual_restante["quantity"] = quantidade_atual
                    compras_pendentes.append(op_atual_restante)

            elif op_atual["operation"] == "sell":
                # Tenta fechar com compras pendentes
                while quantidade_atual > 0 and compras_pendentes:
                    compra_pendente = compras_pendentes[0]
                    qtd_fechar = min(quantidade_atual, compra_pendente["quantity"])

                    op_fechada = _criar_operacao_fechada_detalhada(
                        op_abertura=compra_pendente, 
                        op_fechamento=op_atual, 
                        quantidade_fechada=qtd_fechar,
                        tipo_fechamento="compra_fechada_com_venda"
                    )
                    operacoes_fechadas_para_salvar.append(op_fechada)
                    
                    compra_pendente["quantity"] -= qtd_fechar
                    quantidade_atual -= qtd_fechar
                    
                    if compra_pendente["quantity"] == 0:
                        compras_pendentes.pop(0)
                
                if quantidade_atual > 0: # Venda a descoberto
                    op_atual_restante = op_atual.copy()
                    op_atual_restante["quantity"] = quantidade_atual
                    vendas_pendentes.append(op_atual_restante)

    # Salva todas as operações fechadas no banco
    for op_f in operacoes_fechadas_para_salvar:
        salvar_operacao_fechada(op_f, usuario_id=usuario_id)
        
    return operacoes_fechadas_para_salvar


def _criar_operacao_fechada_detalhada(op_abertura: Dict, op_fechamento: Dict, quantidade_fechada: int, tipo_fechamento: str) -> Dict:
    """
    Cria um dicionário detalhado para uma operação fechada, alinhado com OperacaoFechada e OperacaoDetalhe.
    """
    # Preços unitários
    preco_unitario_abertura = op_abertura["price"]
    preco_unitario_fechamento = op_fechamento["price"]
    
    # Taxas proporcionais
    taxas_proporcionais_abertura = (op_abertura["fees"] / op_abertura["quantity"]) * quantidade_fechada if op_abertura["quantity"] > 0 else 0
    taxas_proporcionais_fechamento = (op_fechamento["fees"] / op_fechamento["quantity"]) * quantidade_fechada if op_fechamento["quantity"] > 0 else 0

    # Valores totais para cálculo do resultado
    valor_total_abertura_calculo = preco_unitario_abertura * quantidade_fechada
    valor_total_fechamento_calculo = preco_unitario_fechamento * quantidade_fechada

    # Determina o tipo e calcula o resultado
    if tipo_fechamento == "compra_fechada_com_venda": # Compra (abertura) e Venda (fechamento)
        tipo_operacao_fechada = "compra-venda"
        resultado_bruto = valor_total_fechamento_calculo - valor_total_abertura_calculo
        resultado_liquido = resultado_bruto - taxas_proporcionais_abertura - taxas_proporcionais_fechamento
        data_ab = op_abertura["date"]
        data_fec = op_fechamento["date"]
    elif tipo_fechamento == "venda_descoberta_fechada_com_compra": # Venda (abertura) e Compra (fechamento)
        tipo_operacao_fechada = "venda-compra"
        resultado_bruto = valor_total_abertura_calculo - valor_total_fechamento_calculo # Venda é abertura (valor maior)
        resultado_liquido = resultado_bruto - taxas_proporcionais_abertura - taxas_proporcionais_fechamento
        data_ab = op_abertura["date"] # Data da venda a descoberto
        data_fec = op_fechamento["date"] # Data da recompra
    else:
        raise ValueError(f"Tipo de fechamento desconhecido: {tipo_fechamento}")

    operacoes_relacionadas = [
        {
            "id": op_abertura.get("id"),
            "date": op_abertura["date"],
            "operation": op_abertura["operation"],
            "quantity": quantidade_fechada,
            "price": preco_unitario_abertura,
            "fees": taxas_proporcionais_abertura,
            "valor_total": preco_unitario_abertura * quantidade_fechada
        },
        {
            "id": op_fechamento.get("id"),
            "date": op_fechamento["date"],
            "operation": op_fechamento["operation"],
            "quantity": quantidade_fechada,
            "price": preco_unitario_fechamento,
            "fees": taxas_proporcionais_fechamento,
            "valor_total": preco_unitario_fechamento * quantidade_fechada
        }
    ]

    return {
        "ticker": op_abertura["ticker"],
        "data_abertura": data_ab,
        "data_fechamento": data_fec,
        "tipo": tipo_operacao_fechada,
        "quantidade": quantidade_fechada,
        "preco_abertura": preco_unitario_abertura,
        "preco_fechamento": preco_unitario_fechamento,
        "taxas_total": taxas_proporcionais_abertura + taxas_proporcionais_fechamento,
        "resultado": resultado_liquido,
        "operacoes_relacionadas": operacoes_relacionadas,
        "day_trade": op_abertura["date"] == op_fechamento["date"]
    }


def recalcular_carteira(usuario_id: int) -> None:
    """
    Recalcula a carteira atual de um usuário com base em todas as suas operações.
    """
    # Obtém todas as operações do usuário
    operacoes = obter_todas_operacoes(usuario_id=usuario_id)
    
    # Dicionário para armazenar a carteira atual
    carteira_temp = defaultdict(lambda: {"quantidade": 0, "custo_total": 0.0, "preco_medio": 0.0})
    
    # Processa cada operação
    for op in operacoes:
        ticker = op["ticker"]
        quantidade_op = op["quantity"]
        valor_op = quantidade_op * op["price"]
        fees_op = op.get("fees", 0.0)

        if op["operation"] == "buy":
            carteira_temp[ticker]["quantidade"] += quantidade_op
            carteira_temp[ticker]["custo_total"] += valor_op + fees_op
        elif op["operation"] == "sell":
            # Custo da venda é baseado no preço médio antes da venda
            # Isso já deve estar refletido no cálculo do resultado da operação fechada.
            # Aqui, apenas ajustamos a quantidade e o custo total restante.
            custo_venda = carteira_temp[ticker]["preco_medio"] * quantidade_op if carteira_temp[ticker]["quantidade"] > 0 else 0
            
            carteira_temp[ticker]["quantidade"] -= quantidade_op
            # O custo total é reduzido pelo custo das ações vendidas (baseado no preço médio)
            carteira_temp[ticker]["custo_total"] -= custo_venda
            # Se a quantidade for zerada, o custo total também deve ser zerado para evitar divisão por zero.
            if carteira_temp[ticker]["quantidade"] <= 0:
                 carteira_temp[ticker]["custo_total"] = 0


        # Recalcula o preço médio após cada operação
        if carteira_temp[ticker]["quantidade"] > 0:
            carteira_temp[ticker]["preco_medio"] = carteira_temp[ticker]["custo_total"] / carteira_temp[ticker]["quantidade"]
        else: # Zerar se não houver mais ações
            carteira_temp[ticker]["preco_medio"] = 0.0
            carteira_temp[ticker]["custo_total"] = 0.0 # Garante que o custo total seja zero
            
    # Atualiza a carteira no banco de dados para o usuário
    for ticker, dados in carteira_temp.items():
        # Salva mesmo se a quantidade for zero para remover da carteira no DB,
        # ou a função atualizar_carteira pode decidir não salvar se quantidade for zero.
        # A função `atualizar_carteira` do database usa INSERT OR REPLACE, 
        # então se a quantidade for 0, ela ainda será salva assim.
        # Se quisermos remover, precisaríamos de uma lógica de DELETE no DB.
        # Por ora, salvar com quantidade zero é aceitável.
        atualizar_carteira(ticker, dados["quantidade"], dados["preco_medio"], usuario_id=usuario_id)


def recalcular_resultados(usuario_id: int) -> None:
    """
    Recalcula os resultados mensais de um usuário com base em todas as suas operações.
    """
    # Obtém todas as operações do usuário
    operacoes = obter_todas_operacoes(usuario_id=usuario_id)
    
    # Agrupa as operações por mês
    operacoes_por_mes = defaultdict(list)
    for op in operacoes:
        # Garante que 'date' seja um objeto date
        op_date = op["date"]
        if isinstance(op_date, str):
            op_date = datetime.fromisoformat(op_date.split("T")[0]).date()
        elif isinstance(op_date, datetime):
            op_date = op_date.date()
        
        mes = op_date.strftime("%Y-%m")
        operacoes_por_mes[mes].append(op)
    
    # Dicionários para armazenar os prejuízos acumulados
    prejuizo_acumulado_swing = 0.0
    prejuizo_acumulado_day = 0.0
    
    # Processa cada mês em ordem cronológica
    for mes_str, ops_mes in sorted(operacoes_por_mes.items()):
        # Agrupa as operações por dia dentro do mês
        operacoes_por_dia = defaultdict(list)
        for op_m in ops_mes:
            op_date_dia = op_m["date"]
            if isinstance(op_date_dia, str): # Converte se necessário
                op_date_dia = datetime.fromisoformat(op_date_dia.split("T")[0]).date()
            elif isinstance(op_date_dia, datetime):
                 op_date_dia = op_date_dia.date()
            dia_iso = op_date_dia.isoformat()
            operacoes_por_dia[dia_iso].append(op_m)
        
        # Inicializa os resultados do mês
        resultado_mes_swing = {"vendas": 0.0, "custo": 0.0, "ganho_liquido": 0.0}
        resultado_mes_day = {"vendas": 0.0, "custo": 0.0, "ganho_liquido": 0.0, "irrf": 0.0}
        
        # Processa cada dia em ordem cronológica
        for dia_str, ops_dia_list in sorted(operacoes_por_dia.items()):
            # Passa usuario_id para _calcular_resultado_dia
            resultado_dia_swing, resultado_dia_day = _calcular_resultado_dia(ops_dia_list, usuario_id=usuario_id)
            
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
        salvar_resultado_mensal(resultado, usuario_id=usuario_id)

def listar_operacoes_service(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Serviço para listar todas as operações de um usuário.
    """
    return obter_todas_operacoes(usuario_id=usuario_id)

def deletar_operacao_service(operacao_id: int, usuario_id: int) -> bool:
    """
    Serviço para deletar uma operação e recalcular carteira e resultados.
    Retorna True se a operação foi deletada, False caso contrário.
    """
    if remover_operacao(operacao_id, usuario_id=usuario_id):
        recalcular_carteira(usuario_id=usuario_id)
        recalcular_resultados(usuario_id=usuario_id)
        return True
    return False

def gerar_resumo_operacoes_fechadas(usuario_id: int) -> Dict[str, Any]:
    """
    Gera um resumo das operações fechadas para um usuário.
    """
    operacoes_fechadas = calcular_operacoes_fechadas(usuario_id=usuario_id)
    
    # Calcula o resumo
    total_operacoes = len(operacoes_fechadas)
    lucro_total = sum(op["resultado"] for op in operacoes_fechadas)
    
    # Separa day trade e swing trade
    operacoes_day_trade = [op for op in operacoes_fechadas if op.get("day_trade", False)]
    operacoes_swing_trade = [op for op in operacoes_fechadas if not op.get("day_trade", False)]
    
    lucro_day_trade = sum(op["resultado"] for op in operacoes_day_trade)
    lucro_swing_trade = sum(op["resultado"] for op in operacoes_swing_trade)
    
    # Encontra as operações mais lucrativas e com maior prejuízo
    # Certifique-se de que 'resultado' existe em cada 'op'
    operacoes_ordenadas = sorted(operacoes_fechadas, key=lambda x: x.get("resultado", 0), reverse=True)
    operacoes_lucrativas = [op for op in operacoes_ordenadas if op.get("resultado", 0) > 0]
    operacoes_prejuizo = [op for op in operacoes_ordenadas if op.get("resultado", 0) < 0] # Corrigido para < 0
    
    top_lucrativas = operacoes_lucrativas[:5]
    top_prejuizo = sorted(operacoes_prejuizo, key=lambda x: x.get("resultado", 0))[:5] # Ordena por menor resultado para pegar os maiores prejuízos

    # Calcula o resumo por ticker
    resumo_por_ticker = defaultdict(lambda: {
        "total_operacoes": 0,
        "lucro_total": 0.0, # Certifique-se que é float
        "operacoes_lucrativas": 0,
        "operacoes_prejuizo": 0
    })
    for op in operacoes_fechadas:
        ticker = op["ticker"]
        resumo_por_ticker[ticker]["total_operacoes"] += 1
        resumo_por_ticker[ticker]["lucro_total"] += op.get("resultado", 0)
        
        if op.get("resultado", 0) > 0:
            resumo_por_ticker[ticker]["operacoes_lucrativas"] += 1
        elif op.get("resultado", 0) < 0:
            resumo_por_ticker[ticker]["operacoes_prejuizo"] += 1
    
    return {
        "total_operacoes": total_operacoes,
        "lucro_total": lucro_total,
        "lucro_day_trade": lucro_day_trade,
        "lucro_swing_trade": lucro_swing_trade,
        "total_day_trade": len(operacoes_day_trade),
        "total_swing_trade": len(operacoes_swing_trade),
        "top_lucrativas": top_lucrativas,
        "top_prejuizo": top_prejuizo,
        "resumo_por_ticker": dict(resumo_por_ticker) # Converte defaultdict para dict para a resposta
    }

# A função recalcular_resultados abaixo do comentário parece ser uma versão mais antiga ou incorreta.
# Vou remover para evitar confusão, pois a de cima já foi atualizada.
# def recalcular_resultados() -> None:
#     """
#     Recalcula os resultados mensais com base em todas as operações.
#     """
#     # Obtém todas as operações
#     operacoes = obter_todas_operacoes()  # Corrigido de obter_todas_operações
    
#     # Dicionário para armazenar resultados mensais
#     resultados_mensais = defaultdict(lambda: {"resultado": 0.0})
    
#     # Processa cada operação
#     for op in operacoes:
#         ticker = op["ticker"]
#         quantidade = op["quantity"]
#         valor = quantidade * op["price"]
#         data = op["date"]
        
#         # Formata a data para o início do mês
#         ano, mes = data.year, data.month
#         mes_primeiro_dia = date(ano, mes, 1)
        
#         if op["operation"] == "buy":
#             # Compra: subtrai do resultado mensal
#             resultados_mensais[mes_primeiro_dia]["resultado"] -= valor + op["fees"]
#         else:
#             # Venda: adiciona ao resultado mensal
#             resultados_mensais[mes_primeiro_dia]["resultado"] += valor - op["fees"]
    
#     # Salva os resultados mensais no banco de dados
#     for data_primeiro_dia, dados in resultados_mensais.items():
#         salvar_resultado_mensal(data_primeiro_dia, dados["resultado"])            