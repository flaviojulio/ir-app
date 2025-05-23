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
    
    # Inicializa o sistema de autenticação
    from auth import inicializar_autenticacao
    inicializar_autenticacao()
    
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
            query += ' WHERE usuario_id = ? OR usuario_id IS NULL'
            params.append(usuario_id)
        
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
        if usuario_id is not None:
            cursor.execute('''
            SELECT id FROM operacoes
            WHERE id = ? AND (usuario_id = ? OR usuario_id IS NULL)
            ''', (operacao_id, usuario_id))
        else:
            cursor.execute('SELECT id FROM operacoes WHERE id = ?', (operacao_id,))
        
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
        if usuario_id is not None:
            cursor.execute('''
            SELECT id FROM operacoes
            WHERE id = ? AND (usuario_id = ? OR usuario_id IS NULL)
            ''', (operacao_id, usuario_id))
        else:
            cursor.execute('SELECT id FROM operacoes WHERE id = ?', (operacao_id,))
        
        if not cursor.fetchone():
            return False
        
        cursor.execute('DELETE FROM operacoes WHERE id = ?', (operacao_id,))
        
        conn.commit()
        
        return cursor.rowcount > 0

def obter_todas_operacoes() -> List[Dict[str, Any]]:
    """
    Obtém todas as operações do banco de dados.
    
    Returns:
        List[Dict[str, Any]]: Lista de operações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM operacoes ORDER BY date')
        
        # Converte os resultados para dicionários
        operacoes = []
        for row in cursor.fetchall():
            operacao = dict(row)
            operacao["date"] = datetime.strptime(operacao["date"], "%Y-%m-%d").date()
            operacoes.append(operacao)
            
        return operacoes

# Função atualizada para receber preço médio em vez de custo total
def atualizar_carteira(ticker: str, quantidade: int, preco_medio: float) -> None:
    """
    Atualiza ou insere um item na carteira atual.
    
    Args:
        ticker: Código da ação.
        quantidade: Quantidade de ações.
        preco_medio: Preço médio das ações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Calcula o custo total a partir da quantidade e do preço médio
        custo_total = quantidade * preco_medio
        
        # Verifica se o ticker já existe na carteira
        cursor.execute('SELECT * FROM carteira_atual WHERE ticker = ?', (ticker,))
        item = cursor.fetchone()
        
        if item:
            # Se o ticker já existe, atualiza os valores
            cursor.execute('''
            UPDATE carteira_atual
            SET quantidade = ?, custo_total = ?, preco_medio = ?
            WHERE ticker = ?
            ''', (
                quantidade,
                custo_total,
                preco_medio,
                ticker
            ))
        else:
            # Se o ticker não existe, insere um novo item
            cursor.execute('''
            INSERT INTO carteira_atual (ticker, quantidade, custo_total, preco_medio)
            VALUES (?, ?, ?, ?)
            ''', (
                ticker,
                quantidade,
                custo_total,
                preco_medio
            ))
        
        conn.commit()
        
def obter_carteira_atual() -> List[Dict[str, Any]]:
    """
    Obtém a carteira atual de ações.
    
    Returns:
        List[Dict[str, Any]]: Lista de itens da carteira.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM carteira_atual ORDER BY ticker')
        
        # Converte os resultados para dicionários
        carteira = [dict(row) for row in cursor.fetchall()]
        
        return carteira

def salvar_resultado_mensal(resultado: Dict[str, Any]) -> int:
    """
    Salva um resultado mensal no banco de dados.
    
    Args:
        resultado: Dicionário com os dados do resultado mensal.
        
    Returns:
        int: ID do resultado inserido.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se já existe um resultado para o mês
        cursor.execute('SELECT id FROM resultados_mensais WHERE mes = ?', (resultado["mes"],))
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
            WHERE mes = ?
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
                resultado["mes"]
            ))
            return existente["id"]
        else:
            # Se não existe, insere
            cursor.execute('''
            INSERT INTO resultados_mensais (
                mes, vendas_swing, custo_swing, ganho_liquido_swing,
                isento_swing, ganho_liquido_day, ir_devido_day,
                irrf_day, ir_pagar_day, prejuizo_acumulado_swing,
                prejuizo_acumulado_day, darf_codigo, darf_competencia,
                darf_valor, darf_vencimento
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                resultado.get("darf_vencimento").isoformat() if resultado.get("darf_vencimento") else None
            ))
            
            conn.commit()
            return cursor.lastrowid
        
def obter_resultados_mensais() -> List[Dict[str, Any]]:
    """
    Obtém todos os resultados mensais do banco de dados.
    
    Returns:
        List[Dict[str, Any]]: Lista de resultados mensais.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM resultados_mensais ORDER BY mes')
        
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
    Remove todos os dados do banco de dados.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Limpa todas as tabelas
        cursor.execute('DELETE FROM operacoes')
        cursor.execute('DELETE FROM resultados_mensais')
        cursor.execute('DELETE FROM carteira_atual')
        
        # Reseta os contadores de autoincremento
        cursor.execute('DELETE FROM sqlite_sequence WHERE name IN ("operacoes", "resultados_mensais", "carteira_atual")')
        
        conn.commit()

def obter_operacoes_fechadas() -> List[Dict[str, Any]]:
    """
    Obtém as operações fechadas (compra seguida de venda ou vice-versa).
    Usa o método FIFO (First In, First Out) para rastrear as operações.
    
    Returns:
        List[Dict[str, Any]]: Lista de operações fechadas.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Obtém todas as operações ordenadas por data
        cursor.execute('SELECT * FROM operacoes ORDER BY date, id')
        
        # Converte os resultados para dicionários
        operacoes = []
        for row in cursor.fetchall():
            operacao = dict(row)
            operacao["date"] = datetime.strptime(operacao["date"], "%Y-%m-%d").date()
            operacoes.append(operacao)
        
        return operacoes