import sqlite3
import json
from datetime import date, datetime
from contextlib import contextmanager
from typing import Dict, List, Any, Union, Optional
from collections import defaultdict

# Caminho para o banco de dados SQLite
DATABASE_FILE = "acoes_ir.db"

@contextmanager
def get_db():
    """
    Contexto para conexão com o banco de dados.
    """
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def criar_tabelas():
    """
    Cria as tabelas necessárias se não existirem e adiciona colunas ausentes.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Tabela de operações
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS operacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            operation TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            fees REAL NOT NULL DEFAULT 0.0
        )
        ''')
        
        # Verificar se a coluna usuario_id existe na tabela operacoes
        cursor.execute("PRAGMA table_info(operacoes)")
        colunas = [info[1] for info in cursor.fetchall()]
        
        # Adicionar a coluna usuario_id se ela não existir
        if 'usuario_id' not in colunas:
            cursor.execute('ALTER TABLE operacoes ADD COLUMN usuario_id INTEGER DEFAULT NULL')
        
        # Tabela de resultados mensais
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS resultados_mensais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mes TEXT NOT NULL,
            vendas_swing REAL NOT NULL,
            custo_swing REAL NOT NULL,
            ganho_liquido_swing REAL NOT NULL,
            isento_swing INTEGER NOT NULL,
            ganho_liquido_day REAL NOT NULL,
            ir_devido_day REAL NOT NULL,
            irrf_day REAL NOT NULL,
            ir_pagar_day REAL NOT NULL,
            prejuizo_acumulado_swing REAL NOT NULL,
            prejuizo_acumulado_day REAL NOT NULL,
            darf_codigo TEXT,
            darf_competencia TEXT,
            darf_valor REAL,
            darf_vencimento TEXT
        )
        ''')
        
        # Verificar se a coluna usuario_id existe na tabela resultados_mensais
        cursor.execute("PRAGMA table_info(resultados_mensais)")
        colunas = [info[1] for info in cursor.fetchall()]
        
        # Adicionar a coluna usuario_id se ela não existir
        if 'usuario_id' not in colunas:
            cursor.execute('ALTER TABLE resultados_mensais ADD COLUMN usuario_id INTEGER DEFAULT NULL')
        
        # Tabela de carteira atual
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS carteira_atual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            custo_total REAL NOT NULL,
            preco_medio REAL NOT NULL,
            UNIQUE(ticker)
        )
        ''')
        
        # Verificar se a coluna usuario_id existe na tabela carteira_atual
        cursor.execute("PRAGMA table_info(carteira_atual)")
        colunas = [info[1] for info in cursor.fetchall()]
        
        # Adicionar a coluna usuario_id se ela não existir
        if 'usuario_id' not in colunas:
            # Primeiro, remover a restrição UNIQUE existente
            cursor.execute('''
            CREATE TABLE carteira_atual_temp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                quantidade INTEGER NOT NULL,
                custo_total REAL NOT NULL,
                preco_medio REAL NOT NULL,
                usuario_id INTEGER DEFAULT NULL,
                UNIQUE(ticker, usuario_id)
            )
            ''')
            
            # Copiar dados da tabela antiga para a nova
            cursor.execute('''
            INSERT INTO carteira_atual_temp (id, ticker, quantidade, custo_total, preco_medio)
            SELECT id, ticker, quantidade, custo_total, preco_medio FROM carteira_atual
            ''')
            
            # Remover tabela antiga
            cursor.execute('DROP TABLE carteira_atual')
            
            # Renomear tabela temporária
            cursor.execute('ALTER TABLE carteira_atual_temp RENAME TO carteira_atual')
        
        # Tabela de operações fechadas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS operacoes_fechadas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_abertura TEXT NOT NULL,
            data_fechamento TEXT NOT NULL,
            ticker TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            valor_compra REAL NOT NULL,
            valor_venda REAL NOT NULL,
            resultado REAL NOT NULL,
            percentual_lucro REAL NOT NULL
        )
        ''')
        
        # Verificar se a coluna usuario_id existe na tabela operacoes_fechadas
        cursor.execute("PRAGMA table_info(operacoes_fechadas)")
        colunas = [info[1] for info in cursor.fetchall()]
        
        # Adicionar a coluna usuario_id se ela não existir
        if 'usuario_id' not in colunas:
            cursor.execute('ALTER TABLE operacoes_fechadas ADD COLUMN usuario_id INTEGER DEFAULT NULL')
        
        # Criar índices para melhorar performance nas consultas
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_date ON operacoes(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_ticker ON operacoes(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_resultados_mensais_mes ON resultados_mensais(mes)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_fechadas_ticker ON operacoes_fechadas(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_fechadas_data_fechamento ON operacoes_fechadas(data_fechamento)')
        
        # Adiciona índices para as colunas usuario_id
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_usuario_id ON operacoes(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_resultados_mensais_usuario_id ON resultados_mensais(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_carteira_atual_usuario_id ON carteira_atual(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_operacoes_fechadas_usuario_id ON operacoes_fechadas(usuario_id)')
        
        conn.commit()
    
    # from auth import inicializar_autenticacao # Removed: Test fixtures should handle auth init.
    # inicializar_autenticacao()
    
def date_converter(obj):
    """
    Conversor de data para JSON.
    """
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def inserir_operacao(operacao: Dict[str, Any], usuario_id: Optional[int] = None) -> int:
    """
    Insere uma operação no banco de dados.
    
    Args:
        operacao: Dicionário com os dados da operação.
        usuario_id: ID do usuário que está criando a operação (opcional).
        
    Returns:
        int: ID da operação inserida.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO operacoes (date, ticker, operation, quantity, price, fees, usuario_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            operacao["date"].isoformat(),
            operacao["ticker"],
            operacao["operation"],
            operacao["quantity"],
            operacao["price"],
            operacao.get("fees", 0.0),
            usuario_id
        ))
        
        conn.commit()
        return cursor.lastrowid

def obter_operacao(operacao_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtém uma operação pelo ID.
    
    Args:
        operacao_id: ID da operação.
        
    Returns:
        Optional[Dict[str, Any]]: Dados da operação ou None se não encontrada.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, date, ticker, operation, quantity, price, fees, usuario_id
        FROM operacoes
        WHERE id = ?
        ''', (operacao_id,))
        
        operacao = cursor.fetchone()
        
        if not operacao:
            return None
        
        return {
            "id": operacao["id"],
            "date": datetime.fromisoformat(operacao["date"]),
            "ticker": operacao["ticker"],
            "operation": operacao["operation"],
            "quantity": operacao["quantity"],
            "price": operacao["price"],
            "fees": operacao["fees"],
            "usuario_id": operacao["usuario_id"]
        }

def obter_todas_operacoes(usuario_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Obtém todas as operações.
    
    Args:
        usuario_id: ID do usuário para filtrar operações (opcional).
        
    Returns:
        List[Dict[str, Any]]: Lista de operações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        query = '''
        SELECT id, date, ticker, operation, quantity, price, fees, usuario_id
        FROM operacoes
        '''
        
        params = []
        
        if usuario_id is not None:
            # query += ' WHERE usuario_id = ? OR usuario_id IS NULL' # Original logic
            query += ' WHERE usuario_id = ?' # Corrected for strict user separation
            params.append(usuario_id)
        else:
            # This case should ideally not be used if operations are always user-specific.
            # If allowing operations without a user (e.g. for global/admin view), 
            # then explicit handling or a different function might be better.
            # For now, if no usuario_id is passed, fetch all. This might need review based on app requirements.
            pass # No WHERE clause, fetches all operations
        
        query += ' ORDER BY date'
        
        cursor.execute(query, params)
        
        operacoes = []
        for operacao in cursor.fetchall():
            operacoes.append({
                "id": operacao["id"],
                "date": datetime.fromisoformat(operacao["date"]),
                "ticker": operacao["ticker"],
                "operation": operacao["operation"],
                "quantity": operacao["quantity"],
                "price": operacao["price"],
                "fees": operacao["fees"],
                "usuario_id": operacao["usuario_id"]
            })
        
        return operacoes

def atualizar_operacao(operacao_id: int, operacao: Dict[str, Any], usuario_id: Optional[int] = None) -> bool:
    """
    Atualiza uma operação.
    
    Args:
        operacao_id: ID da operação.
        operacao: Dicionário com os novos dados.
        usuario_id: ID do usuário para verificação de permissão (opcional).
        
    Returns:
        bool: True se a operação foi atualizada, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se a operação existe e se pertence ao usuário (se especificado)
        # Corrected logic for user-specific check:
        query = 'SELECT id FROM operacoes WHERE id = ?'
        params = [operacao_id]
        if usuario_id is not None:
            query += ' AND usuario_id = ?'
            params.append(usuario_id)
        
        cursor.execute(query, tuple(params))
        
        if not cursor.fetchone():
            return False
        
        cursor.execute('''
        UPDATE operacoes
        SET date = ?, ticker = ?, operation = ?, quantity = ?, price = ?, fees = ?
        WHERE id = ?
        ''', (
            operacao["date"].isoformat(),
            operacao["ticker"],
            operacao["operation"],
            operacao["quantity"],
            operacao["price"],
            operacao.get("fees", 0.0),
            operacao_id
        ))
        
        conn.commit()
        
        return cursor.rowcount > 0

def remover_operacao(operacao_id: int, usuario_id: Optional[int] = None) -> bool:
    """
    Remove uma operação.
    
    Args:
        operacao_id: ID da operação.
        usuario_id: ID do usuário para verificação de permissão (opcional).
        
    Returns:
        bool: True se a operação foi removida, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se a operação existe e se pertence ao usuário (se especificado)
        check_query = "SELECT id FROM operacoes WHERE id = ?"
        check_params = [operacao_id]
        if usuario_id is not None:
            check_query += " AND usuario_id = ?"
            check_params.append(usuario_id)
        
        cursor.execute(check_query, tuple(check_params))
        
        if not cursor.fetchone():
            return False
        
        cursor.execute('DELETE FROM operacoes WHERE id = ?', (operacao_id,))
        
        conn.commit()
        
        return cursor.rowcount > 0

# REMOVED DUPLICATE obter_todas_operacoes aound line 344

# Função atualizada para receber preço médio em vez de custo total e usuario_id
def atualizar_carteira(ticker: str, quantidade: int, preco_medio: float, usuario_id: int) -> None:
    """
    Atualiza ou insere um item na carteira atual para um usuário específico.
    
    Args:
        ticker: Código da ação.
        quantidade: Quantidade de ações.
        preco_medio: Preço médio das ações.
        usuario_id: ID do usuário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Calcula o custo total a partir da quantidade e do preço médio
        custo_total = quantidade * preco_medio
        
        # Verifica se o ticker já existe na carteira para o usuário
        cursor.execute('SELECT * FROM carteira_atual WHERE ticker = ? AND usuario_id = ?', (ticker, usuario_id))
        item = cursor.fetchone()
        
        if item:
            # Se o ticker já existe, atualiza os valores
            cursor.execute('''
            UPDATE carteira_atual
            SET quantidade = ?, custo_total = ?, preco_medio = ?
            WHERE ticker = ? AND usuario_id = ?
            ''', (
                quantidade,
                custo_total,
                preco_medio,
                ticker,
                usuario_id
            ))
        else:
            # Se o ticker não existe, insere um novo item
            cursor.execute('''
            INSERT INTO carteira_atual (ticker, quantidade, custo_total, preco_medio, usuario_id)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                ticker,
                quantidade,
                custo_total,
                preco_medio,
                usuario_id
            ))
        
        conn.commit()
        
def obter_carteira_atual(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém a carteira atual de ações para um usuário específico.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de itens da carteira.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM carteira_atual WHERE usuario_id = ? ORDER BY ticker', (usuario_id,))
        
        # Converte os resultados para dicionários
        carteira = [dict(row) for row in cursor.fetchall()]
        
        return carteira

def salvar_resultado_mensal(resultado: Dict[str, Any], usuario_id: int) -> int:
    """
    Salva um resultado mensal no banco de dados para um usuário específico.
    
    Args:
        resultado: Dicionário com os dados do resultado mensal.
        usuario_id: ID do usuário.
        
    Returns:
        int: ID do resultado inserido ou atualizado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se já existe um resultado para o mês e usuário
        cursor.execute('SELECT id FROM resultados_mensais WHERE mes = ? AND usuario_id = ?', (resultado["mes"], usuario_id))
        existente = cursor.fetchone()
        
        if existente:
            # Se já existe, atualiza
            cursor.execute('''
            UPDATE resultados_mensais
            SET vendas_swing = ?, custo_swing = ?, ganho_liquido_swing = ?,
                isento_swing = ?, ganho_liquido_day = ?, ir_devido_day = ?,
                irrf_day = ?, ir_pagar_day = ?, prejuizo_acumulado_swing = ?,
                prejuizo_acumulado_day = ?, darf_codigo = ?, darf_competencia = ?,
                darf_valor = ?, darf_vencimento = ?
            WHERE mes = ? AND usuario_id = ?
            ''', (
                resultado["vendas_swing"],
                resultado["custo_swing"],
                resultado["ganho_liquido_swing"],
                1 if resultado["isento_swing"] else 0,
                resultado["ganho_liquido_day"],
                resultado["ir_devido_day"],
                resultado["irrf_day"],
                resultado["ir_pagar_day"],
                resultado["prejuizo_acumulado_swing"],
                resultado["prejuizo_acumulado_day"],
                resultado.get("darf_codigo"),
                resultado.get("darf_competencia"),
                resultado.get("darf_valor"),
                resultado.get("darf_vencimento").isoformat() if resultado.get("darf_vencimento") else None,
                resultado["mes"],
                usuario_id # Added for WHERE clause
            ))
            conn.commit() # Ensure commit after update
            return existente["id"]
        else:
            # Se não existe, insere
            cursor.execute('''
            INSERT INTO resultados_mensais (
                mes, vendas_swing, custo_swing, ganho_liquido_swing,
                isento_swing, ganho_liquido_day, ir_devido_day,
                irrf_day, ir_pagar_day, prejuizo_acumulado_swing,
                prejuizo_acumulado_day, darf_codigo, darf_competencia,
                darf_valor, darf_vencimento, usuario_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                resultado["mes"],
                resultado["vendas_swing"],
                resultado["custo_swing"],
                resultado["ganho_liquido_swing"],
                1 if resultado["isento_swing"] else 0,
                resultado["ganho_liquido_day"],
                resultado["ir_devido_day"],
                resultado["irrf_day"],
                resultado["ir_pagar_day"],
                resultado["prejuizo_acumulado_swing"],
                resultado["prejuizo_acumulado_day"],
                resultado.get("darf_codigo"),
                resultado.get("darf_competencia"),
                resultado.get("darf_valor"),
                resultado.get("darf_vencimento").isoformat() if resultado.get("darf_vencimento") else None,
                usuario_id # Added usuario_id
            ))
            
            conn.commit()
            return cursor.lastrowid
        
def obter_resultados_mensais(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém todos os resultados mensais do banco de dados para um usuário específico.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de resultados mensais.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM resultados_mensais WHERE usuario_id = ? ORDER BY mes', (usuario_id,))
        
        # Converte os resultados para dicionários
        resultados = []
        for row in cursor.fetchall():
            resultado = dict(row)
            resultado["isento_swing"] = bool(resultado["isento_swing"])
            if resultado["darf_vencimento"]:
                resultado["darf_vencimento"] = datetime.strptime(resultado["darf_vencimento"], "%Y-%m-%d").date()
            resultados.append(resultado)
            
        return resultados

def limpar_banco_dados() -> None:
    """
    Remove todos os dados das tabelas de aplicação (operações, resultados, carteira).
    Não remove dados de autenticação (usuários, tokens, etc.).
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Limpa todas as tabelas de aplicação
        cursor.execute('DELETE FROM operacoes')
        cursor.execute('DELETE FROM resultados_mensais')
        cursor.execute('DELETE FROM carteira_atual')
        cursor.execute('DELETE FROM operacoes_fechadas')
        
        # Reseta os contadores de autoincremento para tabelas de aplicação
        cursor.execute('DELETE FROM sqlite_sequence WHERE name IN ("operacoes", "resultados_mensais", "carteira_atual", "operacoes_fechadas")')
        
        conn.commit()

def obter_operacoes_fechadas(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém as operações (para cálculo de fechadas) para um usuário específico.
    Usa o método FIFO (First In, First Out) para rastrear as operações.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de operações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Obtém todas as operações para o usuário específico, ordenadas por data
        cursor.execute('SELECT * FROM operacoes WHERE usuario_id = ? ORDER BY date, id', (usuario_id,))
        
        # Converte os resultados para dicionários
        operacoes = []
        for row in cursor.fetchall():
            operacao = dict(row)
            # Ensure date conversion using fromisoformat, similar to obter_todas_operacoes
            if isinstance(operacao["date"], str):
                 operacao["date"] = datetime.fromisoformat(operacao["date"]).date()
            operacoes.append(operacao)
        
        return operacoes