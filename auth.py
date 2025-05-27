"""
Módulo de autenticação e autorização.
Este módulo contém funções para gerenciar usuários, autenticação e controle de acesso.
"""

import hashlib # Standard library
import secrets # Standard library
import time    # Standard library
import jwt     # Third-party
import os      # Standard library (for getenv)
from datetime import datetime, timedelta # Standard library
from typing import Dict, List, Any, Optional # Standard library
# sqlite3, Tuple, contextmanager were unused directly in this file. get_db handles its own context.

# Importa a função get_db do módulo database
from database import get_db

# Custom Exception Classes for Token Handling
class TokenExpiredError(Exception):
    """Raised when a token has expired."""
    pass

class InvalidTokenError(Exception):
    """Raised when a token is invalid or malformed."""
    pass

class TokenNotFoundError(Exception):
    """Raised when a token is not found in the database."""
    pass

class TokenRevokedError(Exception):
    """Raised when a token has been revoked."""
    pass

# Constantes para configuração
import os
JWT_SECRET = os.getenv("JWT_SECRET", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjEsImlhdCI6MTc0Nzk1ODg2MiwiZXhwIjoxNzQ4MDQ1MjYyLCJyb2xlcyI6WyJhZG1pbiIsInVzdWFyaW8iXX0.b5B-TYv_fICuYvcG7fT7cGgSiKfSwDPuT6iBCB9UfVI")  # Defina no ambiente ou use um valor fixo para testes
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION = 24 * 60 * 60

def criar_tabelas_autenticacao() -> None:
    """
    Cria as tabelas necessárias para autenticação e autorização.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Tabela de usuários
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            senha_hash TEXT NOT NULL,
            senha_salt TEXT NOT NULL,
            nome_completo TEXT,
            data_criacao TEXT NOT NULL,
            data_atualizacao TEXT NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1,
            email_verificado INTEGER NOT NULL DEFAULT 0
        )
        ''')
        
        # Tabela de funções (roles)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funcoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            descricao TEXT
        )
        ''')
        
        # Tabela de relação entre usuários e funções
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuario_funcoes (
            usuario_id INTEGER NOT NULL,
            funcao_id INTEGER NOT NULL,
            PRIMARY KEY (usuario_id, funcao_id),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
            FOREIGN KEY (funcao_id) REFERENCES funcoes(id) ON DELETE CASCADE
        )
        ''')
        
        # Tabela de tokens de autenticação
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            data_criacao TEXT NOT NULL,
            data_expiracao TEXT NOT NULL,
            revogado INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
        )
        ''')
        
        # Criar índices para melhorar performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_username ON usuarios(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_email ON usuarios(email)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_token ON tokens(token)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tokens_usuario_id ON tokens(usuario_id)')
        
        # Inserir funções padrão
        cursor.execute('INSERT OR IGNORE INTO funcoes (nome, descricao) VALUES (?, ?)',
                      ('admin', 'Administrador com acesso completo ao sistema'))
        cursor.execute('INSERT OR IGNORE INTO funcoes (nome, descricao) VALUES (?, ?)',
                      ('usuario', 'Usuário padrão com acesso limitado'))

        # Tabela de redefinição de senha
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS redefinicao_senha (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            data_criacao TEXT NOT NULL,
            data_expiracao TEXT NOT NULL,
            utilizado INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
        )
        ''')
        
        # Índice para redefinição de senha
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_redefinicao_senha_token ON redefinicao_senha(token)')
        
        conn.commit()

def modificar_tabelas_existentes() -> None:
    """
    Modifica as tabelas existentes para incluir referência ao usuário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se a coluna usuario_id já existe na tabela operacoes
        cursor.execute("PRAGMA table_info(operacoes)")
        colunas_operacoes = [coluna[1] for coluna in cursor.fetchall()]
        
        if 'usuario_id' not in colunas_operacoes:
            # Adiciona a coluna usuario_id à tabela operacoes
            cursor.execute('''
            ALTER TABLE operacoes ADD COLUMN usuario_id INTEGER DEFAULT NULL
            ''')
        
        # Verifica se a coluna usuario_id já existe na tabela resultados_mensais
        cursor.execute("PRAGMA table_info(resultados_mensais)")
        colunas_resultados = [coluna[1] for coluna in cursor.fetchall()]
        
        if 'usuario_id' not in colunas_resultados:
            # Adiciona a coluna usuario_id à tabela resultados_mensais
            cursor.execute('''
            ALTER TABLE resultados_mensais ADD COLUMN usuario_id INTEGER DEFAULT NULL
            ''')
        
        # Verifica se a coluna usuario_id já existe na tabela carteira_atual
        cursor.execute("PRAGMA table_info(carteira_atual)")
        colunas_carteira = [coluna[1] for coluna in cursor.fetchall()]
        
        if 'usuario_id' not in colunas_carteira:
            # Adiciona a coluna usuario_id à tabela carteira_atual
            cursor.execute('''
            ALTER TABLE carteira_atual ADD COLUMN usuario_id INTEGER DEFAULT NULL
            ''')
            
            # Modifica a chave única para incluir usuario_id
            # Verifica se a tabela temporária já existe e a remove se necessário
            cursor.execute("DROP TABLE IF EXISTS carteira_atual_temp")
            
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
            
            # Copia os dados da tabela antiga para a nova
            cursor.execute('''
            INSERT INTO carteira_atual_temp (id, ticker, quantidade, custo_total, preco_medio, usuario_id)
            SELECT id, ticker, quantidade, custo_total, preco_medio, usuario_id FROM carteira_atual
            ''')
            
            # Remove a tabela antiga
            cursor.execute('DROP TABLE carteira_atual')
            
            # Renomeia a tabela temporária
            cursor.execute('ALTER TABLE carteira_atual_temp RENAME TO carteira_atual')
            
            # Cria índice para a tabela carteira_atual
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_carteira_atual_usuario_id ON carteira_atual(usuario_id)')
        
        conn.commit()

def gerar_salt() -> str:
    """
    Gera um salt aleatório para hash de senha.
    
    Returns:
        str: Salt em formato hexadecimal.
    """
    return secrets.token_hex(16)

def hash_senha(senha: str, salt: str) -> str:
    """
    Gera um hash seguro para a senha usando PBKDF2.
    
    Args:
        senha: Senha em texto plano.
        salt: Salt para o hash.
        
    Returns:
        str: Hash da senha em formato hexadecimal.
    """
    # Usa PBKDF2 com SHA-256, 100.000 iterações
    key = hashlib.pbkdf2_hmac(
        'sha256',
        senha.encode('utf-8'),
        bytes.fromhex(salt),
        100000
    )
    return key.hex()

def criar_usuario(username: str, email: str, senha: str, nome_completo: Optional[str] = None) -> int:
    """
    Cria um novo usuário no banco de dados.
    
    Args:
        username: Nome de usuário único.
        email: Endereço de e-mail único.
        senha: Senha em texto plano (será armazenada com hash).
        nome_completo: Nome completo do usuário (opcional).
        
    Returns:
        int: ID do usuário criado.
        
    Raises:
        ValueError: Se o username ou email já existirem.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se o username já existe
        cursor.execute('SELECT id FROM usuarios WHERE username = ?', (username,))
        if cursor.fetchone():
            raise ValueError(f"Username '{username}' já está em uso")
        
        # Verifica se o email já existe
        cursor.execute('SELECT id FROM usuarios WHERE email = ?', (email,))
        if cursor.fetchone():
            raise ValueError(f"Email '{email}' já está em uso")
        
        # Gera salt e hash da senha
        salt = gerar_salt()
        senha_hash = hash_senha(senha, salt)
        
        # Data atual
        data_atual = datetime.now().isoformat()
        
        # Insere o usuário
        cursor.execute('''
        INSERT INTO usuarios (username, email, senha_hash, senha_salt, nome_completo, data_criacao, data_atualizacao)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            username,
            email,
            senha_hash,
            salt,
            nome_completo,
            data_atual,
            data_atual
        ))
        
        usuario_id = cursor.lastrowid
        
        # Atribui a função 'usuario' por padrão
        cursor.execute('SELECT id FROM funcoes WHERE nome = ?', ('usuario',))
        funcao_id = cursor.fetchone()[0]
        
        cursor.execute('INSERT INTO usuario_funcoes (usuario_id, funcao_id) VALUES (?, ?)',
                      (usuario_id, funcao_id))
        
        conn.commit()
        
        return usuario_id

def atualizar_usuario(usuario_id: int, dados: Dict[str, Any]) -> bool:
    """
    Atualiza os dados de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        dados: Dicionário com os dados a serem atualizados.
            Chaves possíveis: username, email, nome_completo, senha, ativo
            
    Returns:
        bool: True se o usuário foi atualizado, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se o usuário existe
        cursor.execute('SELECT * FROM usuarios WHERE id = ?', (usuario_id,))
        usuario = cursor.fetchone()
        
        if not usuario:
            return False
        
        # Prepara os campos e valores para atualização
        campos = []
        valores = []
        
        if 'username' in dados:
            # Verifica se o novo username já está em uso
            cursor.execute('SELECT id FROM usuarios WHERE username = ? AND id != ?', 
                          (dados['username'], usuario_id))
            if cursor.fetchone():
                raise ValueError(f"Username '{dados['username']}' já está em uso")
            
            campos.append('username = ?')
            valores.append(dados['username'])
        
        if 'email' in dados:
            # Verifica se o novo email já está em uso
            cursor.execute('SELECT id FROM usuarios WHERE email = ? AND id != ?', 
                          (dados['email'], usuario_id))
            if cursor.fetchone():
                raise ValueError(f"Email '{dados['email']}' já está em uso")
            
            campos.append('email = ?')
            valores.append(dados['email'])
        
        if 'nome_completo' in dados:
            campos.append('nome_completo = ?')
            valores.append(dados['nome_completo'])
        
        if 'senha' in dados:
            salt = gerar_salt()
            senha_hash = hash_senha(dados['senha'], salt)
            
            campos.append('senha_hash = ?')
            valores.append(senha_hash)
            
            campos.append('senha_salt = ?')
            valores.append(salt)
            
            # Revoga todos os tokens do usuário
            cursor.execute('UPDATE tokens SET revogado = 1 WHERE usuario_id = ?', (usuario_id,))
        
        if 'ativo' in dados:
            campos.append('ativo = ?')
            valores.append(1 if dados['ativo'] else 0)
            
            # Se o usuário for desativado, revoga todos os tokens
            if not dados['ativo']:
                cursor.execute('UPDATE tokens SET revogado = 1 WHERE usuario_id = ?', (usuario_id,))
        
        # Atualiza a data de atualização
        campos.append('data_atualizacao = ?')
        valores.append(datetime.now().isoformat())
        
        # Se não há campos para atualizar, retorna True
        if not campos:
            return True
        
        # Monta a query de atualização
        query = f"UPDATE usuarios SET {', '.join(campos)} WHERE id = ?"
        valores.append(usuario_id)
        
        cursor.execute(query, valores)
        conn.commit()
        
        return True

def excluir_usuario(usuario_id: int) -> bool:
    """
    Exclui um usuário do banco de dados.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        bool: True se o usuário foi excluído, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se o usuário existe
        cursor.execute('SELECT id FROM usuarios WHERE id = ?', (usuario_id,))
        if not cursor.fetchone():
            return False
        
        # Exclui o usuário
        cursor.execute('DELETE FROM usuarios WHERE id = ?', (usuario_id,))
        
        # As tabelas relacionadas serão limpas automaticamente devido às restrições ON DELETE CASCADE
        
        conn.commit()
        
        return cursor.rowcount > 0

def obter_usuario(usuario_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtém os dados de um usuário pelo ID.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        Optional[Dict[str, Any]]: Dados do usuário ou None se não encontrado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, username, email, nome_completo, data_criacao, data_atualizacao, ativo
        FROM usuarios
        WHERE id = ?
        ''', (usuario_id,))
        
        usuario = cursor.fetchone()
        
        if not usuario:
            return None
        
        # Converte para dicionário
        usuario_dict = dict(usuario)
        
        # Obtém as funções do usuário
        cursor.execute('''
        SELECT f.nome
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ?
        ''', (usuario_id,))
        
        funcoes = [row[0] for row in cursor.fetchall()]
        usuario_dict['funcoes'] = funcoes
        
        return usuario_dict

def obter_usuario_por_username(username: str) -> Optional[Dict[str, Any]]:
    """
    Obtém os dados de um usuário pelo username.
    
    Args:
        username: Username do usuário.
        
    Returns:
        Optional[Dict[str, Any]]: Dados do usuário ou None se não encontrado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, username, email, nome_completo, data_criacao, data_atualizacao, ativo
        FROM usuarios
        WHERE username = ?
        ''', (username,))
        
        usuario = cursor.fetchone()
        
        if not usuario:
            return None
        
        # Converte para dicionário
        usuario_dict = dict(usuario)
        
        # Obtém as funções do usuário
        cursor.execute('''
        SELECT f.nome
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ?
        ''', (usuario_dict['id'],))
        
        funcoes = [row[0] for row in cursor.fetchall()]
        usuario_dict['funcoes'] = funcoes
        
        return usuario_dict

def obter_todos_usuarios() -> List[Dict[str, Any]]:
    """
    Obtém todos os usuários do banco de dados.
    
    Returns:
        List[Dict[str, Any]]: Lista de usuários.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, username, email, nome_completo, data_criacao, data_atualizacao, ativo
        FROM usuarios
        ORDER BY username
        ''')
        
        usuarios = []
        for row in cursor.fetchall():
            usuario = dict(row)
            
            # Obtém as funções do usuário
            cursor.execute('''
            SELECT f.nome
            FROM usuario_funcoes uf
            JOIN funcoes f ON uf.funcao_id = f.id
            WHERE uf.usuario_id = ?
            ''', (usuario['id'],))
            
            funcoes = [row[0] for row in cursor.fetchall()]
            usuario['funcoes'] = funcoes
            
            usuarios.append(usuario)
        
        return usuarios

def verificar_credenciais(username_ou_email: str, senha: str) -> Optional[Dict[str, Any]]:
    """
    Verifica as credenciais de um usuário.
    
    Args:
        username_ou_email: Username ou email do usuário.
        senha: Senha em texto plano.
        
    Returns:
        Optional[Dict[str, Any]]: Dados do usuário se as credenciais forem válidas, None caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Busca o usuário pelo username ou email
        cursor.execute('''
        SELECT id, username, email, senha_hash, senha_salt, nome_completo, ativo
        FROM usuarios
        WHERE (username = ? OR email = ?)
        ''', (username_ou_email, username_ou_email))
        
        usuario = cursor.fetchone()
        
        if not usuario:
            return None
        
        # Verifica se o usuário está ativo
        if not usuario['ativo']:
            return None
        
        # Verifica a senha
        senha_hash = hash_senha(senha, usuario['senha_salt'])
        
        if senha_hash != usuario['senha_hash']:
            return None
        
        # Retorna os dados do usuário (sem a senha)
        usuario_dict = {
            'id': usuario['id'],
            'username': usuario['username'],
            'email': usuario['email'],
            'nome_completo': usuario['nome_completo']
        }
        
        # Obtém as funções do usuário
        cursor.execute('''
        SELECT f.nome
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ?
        ''', (usuario['id'],))
        
        funcoes = [row[0] for row in cursor.fetchall()]
        usuario_dict['funcoes'] = funcoes
        
        return usuario_dict

def gerar_token(usuario_id: int) -> str:
    """
    Gera um token JWT para um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        str: Token JWT.
    """
    # Obtém as funções do usuário
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT f.nome
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ?
        ''', (usuario_id,))
        
        funcoes = [row[0] for row in cursor.fetchall()]
    
    # Gera o payload do token
    agora = int(time.time())
    expiracao = agora + JWT_EXPIRATION
    
    payload = {
        'sub': str(usuario_id), # Convertido para string
        'iat': agora,
        'exp': expiracao,
        'roles': funcoes
    }
    
    # Gera o token
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    # Salva o token no banco de dados
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO tokens (usuario_id, token, data_criacao, data_expiracao)
        VALUES (?, ?, ?, ?)
        ''', (
            usuario_id,
            token,
            datetime.fromtimestamp(agora).isoformat(),
            datetime.fromtimestamp(expiracao).isoformat()
        ))
        
        conn.commit()
    
    return token

def verificar_token(token: str) -> Dict[str, Any]:
    """
    Verifica um token JWT.
    
    Args:
        token: Token JWT.
        
    Returns:
        Dict[str, Any]: Payload do token se válido.
        
    Raises:
        TokenNotFoundError: Se o token não for encontrado no banco de dados.
        TokenRevokedError: Se o token foi revogado.
        TokenExpiredError: Se o token expirou.
        InvalidTokenError: Se o token for inválido ou malformado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, revogado 
        FROM tokens
        WHERE token = ?
        ''', (token,))
        token_data = cursor.fetchone()

        if not token_data:
            raise TokenNotFoundError("Token not found in database")

        if token_data['revogado']:
            raise TokenRevokedError("Token has been revoked")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        # No need to check 'exp' here, jwt.decode will raise ExpiredSignatureError
        return payload
    
    except jwt.ExpiredSignatureError:
        # Marcar o token como revogado no banco de dados ao expirar
        with get_db() as conn:
            cursor = conn.cursor()
            # Certifique-se de que o token_data (e, portanto, token_data['id']) está disponível aqui
            # ou busque o ID novamente se necessário.
            # Como já verificamos e o token existe e não foi revogado, podemos usar o ID.
            # No entanto, para segurança, vamos verificar se token_data existe (já feito acima)
            # e obter o ID do token para revogação.
            # A lógica original buscava 'id' se 'token_data' fosse None, mas aqui já sabemos que existe.
            if token_data and 'id' in token_data: # 'id' foi adicionado ao SELECT
                 cursor.execute('UPDATE tokens SET revogado = 1 WHERE id = ?', (token_data['id'],))
                 conn.commit()
            else:
                # Este caso não deveria acontecer se o token foi encontrado inicialmente
                # mas é uma salvaguarda.
                # Se não tivermos o ID, podemos tentar revogar pelo token string,
                # mas é menos eficiente e já deveria ter sido tratado.
                # Para este exemplo, vamos assumir que token_data['id'] está disponível.
                pass # Comment about logging a warning if token_data['id'] is not available can be removed for cleanup.

        raise TokenExpiredError("Token has expired")
    
    except jwt.PyJWTError as e: # Captura outras exceções do PyJWT
        raise InvalidTokenError(str(e)) # The original error 'e' is included in the exception.

def revogar_token(token: str) -> bool:
    """
    Revoga um token JWT.
    
    Args:
        token: Token JWT.
        
    Returns:
        bool: True se o token foi revogado, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('UPDATE tokens SET revogado = 1 WHERE token = ?', (token,))
        
        conn.commit()
        
        return cursor.rowcount > 0

def revogar_todos_tokens_usuario(usuario_id: int) -> int:
    """
    Revoga todos os tokens de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        int: Número de tokens revogados.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('UPDATE tokens SET revogado = 1 WHERE usuario_id = ?', (usuario_id,))
        
        conn.commit()
        
        return cursor.rowcount

def adicionar_funcao_usuario(usuario_id: int, funcao_nome: str) -> bool:
    """
    Adiciona uma função a um usuário.
    
    Args:
        usuario_id: ID do usuário.
        funcao_nome: Nome da função.
        
    Returns:
        bool: True se a função foi adicionada, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se o usuário existe
        cursor.execute('SELECT id FROM usuarios WHERE id = ?', (usuario_id,))
        if not cursor.fetchone():
            return False
        
        # Verifica se a função existe
        cursor.execute('SELECT id FROM funcoes WHERE nome = ?', (funcao_nome,))
        funcao = cursor.fetchone()
        
        if not funcao:
            return False
        
        # Verifica se o usuário já tem a função
        cursor.execute('''
        SELECT 1 FROM usuario_funcoes
        WHERE usuario_id = ? AND funcao_id = ?
        ''', (usuario_id, funcao['id']))
        
        if cursor.fetchone():
            return True  # Usuário já tem a função
        
        # Adiciona a função ao usuário
        cursor.execute('''
        INSERT INTO usuario_funcoes (usuario_id, funcao_id)
        VALUES (?, ?)
        ''', (usuario_id, funcao['id']))
        
        conn.commit()
        
        return True

def remover_funcao_usuario(usuario_id: int, funcao_nome: str) -> bool:
    """
    Remove uma função de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        funcao_nome: Nome da função.
        
    Returns:
        bool: True se a função foi removida, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se a função existe
        cursor.execute('SELECT id FROM funcoes WHERE nome = ?', (funcao_nome,))
        funcao = cursor.fetchone()
        
        if not funcao:
            return False
        
        # Remove a função do usuário
        cursor.execute('''
        DELETE FROM usuario_funcoes
        WHERE usuario_id = ? AND funcao_id = ?
        ''', (usuario_id, funcao['id']))
        
        conn.commit()
        
        return cursor.rowcount > 0

def usuario_tem_funcao(usuario_id: int, funcao_nome: str) -> bool:
    """
    Verifica se um usuário tem uma determinada função.
    
    Args:
        usuario_id: ID do usuário.
        funcao_nome: Nome da função.
        
    Returns:
        bool: True se o usuário tem a função, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT 1 FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ? AND f.nome = ?
        ''', (usuario_id, funcao_nome))
        
        return cursor.fetchone() is not None

def criar_funcao(nome: str, descricao: Optional[str] = None) -> int:
    """
    Cria uma nova função no sistema.
    
    Args:
        nome: Nome da função.
        descricao: Descrição da função (opcional).
        
    Returns:
        int: ID da função criada.
        
    Raises:
        ValueError: Se o nome da função já existir.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verifica se a função já existe
        cursor.execute('SELECT id FROM funcoes WHERE nome = ?', (nome,))
        if cursor.fetchone():
            raise ValueError(f"Função '{nome}' já existe")
        
        # Insere a função
        cursor.execute('''
        INSERT INTO funcoes (nome, descricao)
        VALUES (?, ?)
        ''', (nome, descricao))
        
        conn.commit()
        
        return cursor.lastrowid

def obter_todas_funcoes() -> List[Dict[str, Any]]:
    """
    Obtém todas as funções do sistema.
    
    Returns:
        List[Dict[str, Any]]: Lista de funções.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, nome, descricao FROM funcoes ORDER BY nome')
        
        return [dict(row) for row in cursor.fetchall()]

def obter_funcao(funcao_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtém os dados de uma função pelo ID.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, nome, descricao FROM funcoes WHERE id = ?', (funcao_id,))
        funcao_data = cursor.fetchone()
        if funcao_data:
            return dict(funcao_data)
        return None

def obter_funcao_por_nome(nome: str) -> Optional[Dict[str, Any]]:
    """
    Obtém os dados de uma função pelo nome.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, nome, descricao FROM funcoes WHERE nome = ?', (nome,))
        funcao_data = cursor.fetchone()
        if funcao_data:
            return dict(funcao_data)
        return None

def atualizar_funcao(funcao_id: int, nome: Optional[str] = None, descricao: Optional[str] = None) -> bool:
    """
    Atualiza o nome e/ou descrição de uma função.

    Args:
        funcao_id: ID da função a ser atualizada.
        nome: Novo nome para a função (opcional).
        descricao: Nova descrição para a função (opcional).

    Returns:
        bool: True se a atualização foi bem-sucedida, False caso contrário.
              Retorna False se a função não for encontrada ou se o novo nome
              causar um conflito de unicidade.
    
    Raises:
        ValueError: Se o novo nome fornecido for uma string vazia, ou se o nome já existir para outra função.
    """
    if nome is not None and not nome.strip():
        raise ValueError("O nome da função não pode ser vazio.")

    with get_db() as conn:
        cursor = conn.cursor()

        # Verifica se a função existe
        cursor.execute("SELECT nome, descricao FROM funcoes WHERE id = ?", (funcao_id,))
        funcao_atual = cursor.fetchone()
        if not funcao_atual:
            return False  # Função não encontrada

        campos_para_atualizar = []
        valores_para_atualizar = []

        if nome is not None and nome.strip() != funcao_atual["nome"]:
            # Verifica se o novo nome já existe para outra função
            cursor.execute("SELECT id FROM funcoes WHERE nome = ? AND id != ?", (nome.strip(), funcao_id))
            if cursor.fetchone():
                raise ValueError(f"O nome da função '{nome.strip()}' já está em uso.")
            campos_para_atualizar.append("nome = ?")
            valores_para_atualizar.append(nome.strip())
        
        if descricao is not None and descricao != funcao_atual["descricao"]:
            campos_para_atualizar.append("descricao = ?")
            valores_para_atualizar.append(descricao)

        if not campos_para_atualizar:
            return True # Nenhum dado para atualizar, considera sucesso

        query_sql = f"UPDATE funcoes SET {', '.join(campos_para_atualizar)} WHERE id = ?"
        valores_para_atualizar.append(funcao_id)

        try:
            cursor.execute(query_sql, tuple(valores_para_atualizar))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
             # Captura erro de unicidade caso a verificação anterior falhe por alguma race condition (improvável em SQLite por padrão, mas boa prática)
            raise ValueError(f"O nome da função '{nome.strip()}' já está em uso.")
        except Exception:
            conn.rollback() # Garante que a transação seja desfeita em caso de outros erros
            raise # Re-levanta a exceção original para depuração

def verificar_funcao_em_uso(funcao_id: int) -> bool:
    """
    Verifica se uma função (role) está atualmente atribuída a algum usuário.

    Args:
        funcao_id: ID da função a ser verificada.

    Returns:
        bool: True se a função estiver em uso, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM usuario_funcoes WHERE funcao_id = ? LIMIT 1", (funcao_id,))
        return cursor.fetchone() is not None

def excluir_funcao(funcao_id: int) -> bool:
    """
    Exclui uma função do sistema, se não estiver em uso.

    Args:
        funcao_id: ID da função a ser excluída.

    Returns:
        bool: True se a função foi excluída com sucesso.
              False se a função não foi encontrada.
    
    Raises:
        ValueError: Se a função estiver atualmente em uso por algum usuário.
    """
    if obter_funcao(funcao_id) is None:
        return False  # Função não encontrada

    if verificar_funcao_em_uso(funcao_id):
        raise ValueError("A função está atualmente em uso e não pode ser excluída.")

    with get_db() as conn:
        cursor = conn.cursor()
        # A restrição ON DELETE CASCADE na tabela usuario_funcoes removerá as associações,
        # mas a verificação acima impede a exclusão se estiver em uso.
        # Se chegarmos aqui, a função existe e não está em uso.
        cursor.execute("DELETE FROM funcoes WHERE id = ?", (funcao_id,))
        conn.commit()
        return cursor.rowcount > 0

def criar_token_redefinicao_senha(email: str) -> Optional[str]:
    """
    Cria um token para redefinição de senha.
    
    Args:
        email: Email do usuário.
        
    Returns:
        Optional[str]: Token de redefinição ou None se o usuário não for encontrado.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Busca o usuário pelo email
        cursor.execute('SELECT id FROM usuarios WHERE email = ?', (email,))
        usuario = cursor.fetchone()
        
        if not usuario:
            return None
        
        # Gera um token aleatório
        token = secrets.token_urlsafe(32)
        
        # Data atual e de expiração (24 horas)
        data_atual = datetime.now()
        data_expiracao = data_atual + timedelta(hours=24)
        
        # Insere o token
        cursor.execute('''
        INSERT INTO redefinicao_senha (usuario_id, token, data_criacao, data_expiracao)
        VALUES (?, ?, ?, ?)
        ''', (
            usuario["id"],
            token,
            data_atual.isoformat(),
            data_expiracao.isoformat()
        ))
        
        conn.commit()
        
        return token

def redefinir_senha(token: str, nova_senha: str) -> bool:
    """
    Redefine a senha de um usuário usando um token de redefinição.
    
    Args:
        token: Token de redefinição.
        nova_senha: Nova senha em texto plano.
        
    Returns:
        bool: True se a senha foi redefinida, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Busca o token
        cursor.execute('''
        SELECT usuario_id, data_expiracao, utilizado
        FROM redefinicao_senha
        WHERE token = ?
        ''', (token,))
        
        redefinicao = cursor.fetchone()
        
        if not redefinicao:
            return False
        
        if redefinicao["utilizado"]:
            return False
        
        # Verifica se o token expirou
        data_expiracao = datetime.fromisoformat(redefinicao["data_expiracao"])
        if data_expiracao < datetime.now():
            return False
        
        # Gera novo salt e hash da senha
        salt = gerar_salt()
        senha_hash = hash_senha(nova_senha, salt)
        
        # Atualiza a senha do usuário
        cursor.execute('''
        UPDATE usuarios
        SET senha_hash = ?, senha_salt = ?, data_atualizacao = ?
        WHERE id = ?
        ''', (
            senha_hash,
            salt,
            datetime.now().isoformat(),
            redefinicao["usuario_id"]
        ))
        
        # Marca o token como utilizado
        cursor.execute('UPDATE redefinicao_senha SET utilizado = 1 WHERE token = ?', (token,))
        
        # Encerra todas as sessões ativas do usuário (revoga tokens JWT)
        revogar_todos_tokens_usuario(redefinicao["usuario_id"])
        
        conn.commit()
        
        return True

def inicializar_autenticacao() -> None:
    """
    Inicializa o sistema de autenticação.
    Cria as tabelas necessárias, modifica tabelas existentes e insere dados iniciais.
    """
    criar_tabelas_autenticacao()
    modificar_tabelas_existentes()  # Adicionado para modificar tabelas existentes
    
    # Verifica se já existe um usuário administrador
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT 1 FROM usuarios u
        JOIN usuario_funcoes uf ON u.id = uf.usuario_id
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE f.nome = 'admin'
        ''')
        
        if not cursor.fetchone():
            # Cria um usuário administrador padrão
            try:
                usuario_id = criar_usuario(
                    username='admin',
                    email='admin@example.com',
                    senha='admin123',  # Senha inicial que deve ser alterada
                    nome_completo='Administrador do Sistema'
                )
                
                # Adiciona a função de administrador
                adicionar_funcao_usuario(usuario_id, 'admin')
                
                print("Usuário administrador criado com sucesso.")
                print("Username: admin")
                print("Senha: admin123")
                print("IMPORTANTE: Altere a senha do administrador após o primeiro login!")
            except ValueError as e:
                # Ignora erro se o usuário já existir
                pass