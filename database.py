import sqlite3
from datetime import date, datetime
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
# Unused imports json, Union, defaultdict removed

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
        
        # Adiciona usuario_id ao INSERT
        cursor.execute('''
        INSERT INTO operacoes (date, ticker, operation, quantity, price, fees, usuario_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            operacao["date"].isoformat() if isinstance(operacao["date"], (datetime, date)) else operacao["date"],
            operacao["ticker"],
            operacao["operation"],
            operacao["quantity"],
            operacao["price"],
            operacao.get("fees", 0.0),
            usuario_id # Garante que usuario_id seja passado
        ))
        
        conn.commit()
        return cursor.lastrowid

def obter_operacao(operacao_id: int, usuario_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtém uma operação pelo ID e usuario_id.
    
    Args:
        operacao_id: ID da operação.
        usuario_id: ID do usuário.
        
    Returns:
        Optional[Dict[str, Any]]: Dados da operação ou None se não encontrada.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, date, ticker, operation, quantity, price, fees, usuario_id
        FROM operacoes
        WHERE id = ? AND usuario_id = ?
        ''', (operacao_id, usuario_id))
        
        operacao = cursor.fetchone()
        
        if not operacao:
            return None
        
        return {
            "id": operacao["id"],
            "date": datetime.fromisoformat(operacao["date"].split("T")[0]).date() if isinstance(operacao["date"], str) else operacao["date"], # Standardize to date object
            "ticker": operacao["ticker"],
            "operation": operacao["operation"],
            "quantity": operacao["quantity"],
            "price": operacao["price"],
            "fees": operacao["fees"],
            "usuario_id": operacao["usuario_id"]
        }

def obter_todas_operacoes(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém todas as operações de um usuário específico.
    
    Args:
        usuario_id: ID do usuário para filtrar operações.
        
    Returns:
        List[Dict[str, Any]]: Lista de operações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Filtra estritamente por usuario_id
        query = '''
        SELECT id, date, ticker, operation, quantity, price, fees, usuario_id
        FROM operacoes
        WHERE usuario_id = ?
        ORDER BY date
        '''
        
        cursor.execute(query, (usuario_id,))
        
        operacoes = []
        for operacao in cursor.fetchall():
            operacoes.append({
                "id": operacao["id"],
                "date": datetime.fromisoformat(operacao["date"].split("T")[0]).date() if isinstance(operacao["date"], str) else operacao["date"], # Standardize to date object
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
        
        # Verifica se a operação existe e pertence ao usuário
        cursor.execute('''
        SELECT id FROM operacoes
        WHERE id = ? AND usuario_id = ?
        ''', (operacao_id, usuario_id))
        
        if not cursor.fetchone():
            return False # Operação não encontrada ou não pertence ao usuário
        
        cursor.execute('''
        UPDATE operacoes
        SET date = ?, ticker = ?, operation = ?, quantity = ?, price = ?, fees = ?
        WHERE id = ? AND usuario_id = ? 
        ''', (
            operacao["date"].isoformat() if isinstance(operacao["date"], (datetime, date)) else operacao["date"],
            operacao["ticker"],
            operacao["operation"],
            operacao["quantity"],
            operacao["price"],
            operacao.get("fees", 0.0),
            operacao_id,
            usuario_id # Garante que a atualização seja no registro do usuário
        ))
        
        conn.commit()
        
        return cursor.rowcount > 0

def remover_operacao(operacao_id: int, usuario_id: int) -> bool:
    """
    Remove uma operação de um usuário específico.
    
    Args:
        operacao_id: ID da operação.
        usuario_id: ID do usuário.
        
    Returns:
        bool: True se a operação foi removida, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Remove a operação apenas se pertencer ao usuário
        cursor.execute('DELETE FROM operacoes WHERE id = ? AND usuario_id = ?', (operacao_id, usuario_id))
        
        conn.commit()
        
        return cursor.rowcount > 0

# Comment about duplicate function already removed as the function itself was removed in prior step.

def atualizar_carteira(ticker: str, quantidade: int, preco_medio: float, usuario_id: int) -> None:
    """
    Atualiza ou insere um item na carteira atual de um usuário.
    
    Args:
        ticker: Código da ação.
        quantidade: Quantidade de ações.
        preco_medio: Preço médio das ações.
        usuario_id: ID do usuário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        custo_total = quantidade * preco_medio
        
        # Usa INSERT OR REPLACE para simplificar (considerando UNIQUE(ticker, usuario_id))
        # A tabela carteira_atual já deve ter a restrição UNIQUE(ticker, usuario_id)
        # e a coluna usuario_id, conforme definido em criar_tabelas e auth.modificar_tabelas_existentes
        cursor.execute('''
        INSERT OR REPLACE INTO carteira_atual (ticker, quantidade, custo_total, preco_medio, usuario_id)
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
    Obtém a carteira atual de ações de um usuário.
    
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
    Salva um resultado mensal no banco de dados para um usuário.
    
    Args:
        resultado: Dicionário com os dados do resultado mensal.
        usuario_id: ID do usuário.
        
    Returns:
        int: ID do resultado inserido ou atualizado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se já existe um resultado para o mês e usuário
        cursor.execute('SELECT id FROM resultados_mensais WHERE mes = ? AND usuario_id = ?', 
                       (resultado["mes"], usuario_id))
        existente = cursor.fetchone()
        
        darf_vencimento_iso = None
        if resultado.get("darf_vencimento"):
            if isinstance(resultado["darf_vencimento"], (datetime, date)):
                darf_vencimento_iso = resultado["darf_vencimento"].isoformat()
            else: # Assume que já é uma string no formato ISO
                darf_vencimento_iso = resultado["darf_vencimento"]

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
                darf_vencimento_iso,
                resultado["mes"],
                usuario_id
            ))
            conn.commit()
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
                darf_vencimento_iso,
                usuario_id
            ))
            
            conn.commit()
            return cursor.lastrowid
        
def obter_resultados_mensais(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém todos os resultados mensais de um usuário do banco de dados.
    
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
                # Tenta converter de string ISO para date, se necessário
                if isinstance(resultado["darf_vencimento"], str):
                    try:
                        resultado["darf_vencimento"] = datetime.fromisoformat(resultado["darf_vencimento"].split("T")[0]).date()
                    except ValueError: # Se já for YYYY-MM-DD
                        resultado["darf_vencimento"] = datetime.strptime(resultado["darf_vencimento"], "%Y-%m-%d").date()
            resultados.append(resultado)
            
        return resultados

def limpar_banco_dados_usuario(usuario_id: int) -> None:
    """
    Remove todos os dados de um usuário específico do banco de dados.
    Não reseta os contadores de autoincremento globais.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Limpa todas as tabelas relacionadas ao usuário
        cursor.execute('DELETE FROM operacoes WHERE usuario_id = ?', (usuario_id,))
        cursor.execute('DELETE FROM resultados_mensais WHERE usuario_id = ?', (usuario_id,))
        cursor.execute('DELETE FROM carteira_atual WHERE usuario_id = ?', (usuario_id,))
        cursor.execute('DELETE FROM operacoes_fechadas WHERE usuario_id = ?', (usuario_id,)) # Adicionado
        
        # Não reseta sqlite_sequence aqui, pois é global.
        # Se precisar resetar para um usuário, seria mais complexo e geralmente não é feito.
        
        conn.commit()

def limpar_banco_dados() -> None:
    """
    Remove todos os dados de TODAS as tabelas (usado por admin).
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Limpa todas as tabelas
        cursor.execute('DELETE FROM operacoes')
        cursor.execute('DELETE FROM resultados_mensais')
        cursor.execute('DELETE FROM carteira_atual')
        cursor.execute('DELETE FROM operacoes_fechadas') # Adicionado
        
        # Reseta os contadores de autoincremento
        cursor.execute('DELETE FROM sqlite_sequence WHERE name IN ("operacoes", "resultados_mensais", "carteira_atual", "operacoes_fechadas")')
        
        conn.commit()


def obter_operacoes_para_calculo_fechadas(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém todas as operações de um usuário para calcular as operações fechadas.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[Dict[str, Any]]: Lista de operações.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Obtém todas as operações do usuário ordenadas por data e ID
        cursor.execute('SELECT * FROM operacoes WHERE usuario_id = ? ORDER BY date, id', (usuario_id,))
        
        operacoes = []
        for row in cursor.fetchall():
            operacao = dict(row)
            # Converte a string de data para objeto date
            if isinstance(operacao["date"], str):
                operacao["date"] = datetime.fromisoformat(operacao["date"].split("T")[0]).date()
            elif isinstance(operacao["date"], datetime): # Caso a data já seja datetime
                 operacao["date"] = operacao["date"].date()
            operacoes.append(operacao)
        
        return operacoes

def salvar_operacao_fechada(op_fechada: Dict[str, Any], usuario_id: int) -> None:
    """
    Salva uma operação fechada no banco de dados.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO operacoes_fechadas (
                data_abertura, data_fechamento, ticker, quantidade,
                valor_compra, valor_venda, resultado, percentual_lucro, usuario_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            op_fechada['data_abertura'].isoformat() if isinstance(op_fechada['data_abertura'], (date, datetime)) else op_fechada['data_abertura'],
            op_fechada['data_fechamento'].isoformat() if isinstance(op_fechada['data_fechamento'], (date, datetime)) else op_fechada['data_fechamento'],
            op_fechada['ticker'],
            op_fechada['quantidade'],
            op_fechada['valor_compra'],
            op_fechada['valor_venda'],
            op_fechada['resultado'],
            op_fechada['percentual_lucro'],
            usuario_id
        ))
        conn.commit()

def obter_operacoes_fechadas_salvas(usuario_id: int) -> List[Dict[str, Any]]:
    """
    Obtém as operações fechadas já salvas no banco de dados para um usuário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM operacoes_fechadas WHERE usuario_id = ? ORDER BY data_fechamento", (usuario_id,))
        ops_fechadas = []
        for row in cursor.fetchall():
            op = dict(row)
            if isinstance(op["data_abertura"], str):
                op["data_abertura"] = datetime.fromisoformat(op["data_abertura"].split("T")[0]).date()
            if isinstance(op["data_fechamento"], str):
                op["data_fechamento"] = datetime.fromisoformat(op["data_fechamento"].split("T")[0]).date()
            ops_fechadas.append(op)
        return ops_fechadas

def limpar_operacoes_fechadas_usuario(usuario_id: int) -> None:
    """
    Limpa as operações fechadas de um usuário antes de recalcular.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM operacoes_fechadas WHERE usuario_id = ?", (usuario_id,))
        conn.commit()