"""
Esquema de banco de dados para autenticação de usuários.
Este módulo define as tabelas e funções necessárias para implementar
a autenticação de usuários no sistema.
"""

import sqlite3
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import secrets
import hashlib
import os
import uuid

# Importa a função get_db do módulo database
from database import get_db

def criar_tabelas_autenticacao() -> None:
    """
    Cria as tabelas necessárias para autenticação de usuários.
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
        
        # Índices para usuários
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_username ON usuarios(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_email ON usuarios(email)')
        
        # Tabela de sessões
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            data_criacao TEXT NOT NULL,
            data_expiracao TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            ativa INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
        )
        ''')
        
        # Índices para sessões
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessoes_token ON sessoes(token)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessoes_usuario_id ON sessoes(usuario_id)')
        
        # Tabela de funções
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funcoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            descricao TEXT
        )
        ''')
        
        # Tabela de junção usuário-função
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuario_funcoes (
            usuario_id INTEGER NOT NULL,
            funcao_id INTEGER NOT NULL,
            PRIMARY KEY (usuario_id, funcao_id),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
            FOREIGN KEY (funcao_id) REFERENCES funcoes(id) ON DELETE CASCADE
        )
        ''')
        
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
        
        # Inserir funções padrão
        cursor.execute('INSERT OR IGNORE INTO funcoes (nome, descricao) VALUES (?, ?)',
                      ('admin', 'Administrador com acesso completo'))
        cursor.execute('INSERT OR IGNORE INTO funcoes (nome, descricao) VALUES (?, ?)',
                      ('usuario', 'Usuário padrão'))
        
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
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS carteira_atual_temp (
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
    return os.urandom(16).hex()

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
        funcao_id = cursor.fetchone()["id"]
        
        cursor.execute('INSERT INTO usuario_funcoes (usuario_id, funcao_id) VALUES (?, ?)',
                      (usuario_id, funcao_id))
        
        conn.commit()
        
        return usuario_id

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
        
        if not usuario["ativo"]:
            return None
        
        # Verifica a senha
        senha_hash = hash_senha(senha, usuario["senha_salt"])
        
        if senha_hash != usuario["senha_hash"]:
            return None
        
        # Retorna os dados do usuário (sem a senha)
        return {
            "id": usuario["id"],
            "username": usuario["username"],
            "email": usuario["email"],
            "nome_completo": usuario["nome_completo"]
        }

def criar_sessao(usuario_id: int, ip_address: Optional[str] = None, user_agent: Optional[str] = None, 
                duracao_dias: int = 30) -> str:
    """
    Cria uma nova sessão para um usuário.
    
    Args:
        usuario_id: ID do usuário.
        ip_address: Endereço IP do cliente (opcional).
        user_agent: User-Agent do cliente (opcional).
        duracao_dias: Duração da sessão em dias.
        
    Returns:
        str: Token de sessão.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Gera um token aleatório
        token = secrets.token_hex(32)
        
        # Data atual e de expiração
        data_atual = datetime.now()
        data_expiracao = data_atual + timedelta(days=duracao_dias)
        
        # Insere a sessão
        cursor.execute('''
        INSERT INTO sessoes (usuario_id, token, data_criacao, data_expiracao, ip_address, user_agent)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            usuario_id,
            token,
            data_atual.isoformat(),
            data_expiracao.isoformat(),
            ip_address,
            user_agent
        ))
        
        conn.commit()
        
        return token

def verificar_sessao(token: str) -> Optional[Dict[str, Any]]:
    """
    Verifica se um token de sessão é válido.
    
    Args:
        token: Token de sessão.
        
    Returns:
        Optional[Dict[str, Any]]: Dados do usuário se o token for válido, None caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Busca a sessão
        cursor.execute('''
        SELECT s.id, s.usuario_id, s.data_expiracao, s.ativa,
               u.username, u.email, u.nome_completo, u.ativo
        FROM sessoes s
        JOIN usuarios u ON s.usuario_id = u.id
        WHERE s.token = ?
        ''', (token,))
        
        sessao = cursor.fetchone()
        
        if not sessao:
            return None
        
        if not sessao["ativa"] or not sessao["ativo"]:
            return None
        
        # Verifica se a sessão expirou
        data_expiracao = datetime.fromisoformat(sessao["data_expiracao"])
        if data_expiracao < datetime.now():
            # Marca a sessão como inativa
            cursor.execute('UPDATE sessoes SET ativa = 0 WHERE id = ?', (sessao["id"],))
            conn.commit()
            return None
        
        # Retorna os dados do usuário
        return {
            "id": sessao["usuario_id"],
            "username": sessao["username"],
            "email": sessao["email"],
            "nome_completo": sessao["nome_completo"]
        }

def encerrar_sessao(token: str) -> bool:
    """
    Encerra uma sessão.
    
    Args:
        token: Token de sessão.
        
    Returns:
        bool: True se a sessão foi encerrada, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('UPDATE sessoes SET ativa = 0 WHERE token = ?', (token,))
        
        conn.commit()
        
        return cursor.rowcount > 0

def obter_funcoes_usuario(usuario_id: int) -> List[str]:
    """
    Obtém as funções de um usuário.
    
    Args:
        usuario_id: ID do usuário.
        
    Returns:
        List[str]: Lista de nomes de funções.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT f.nome
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ?
        ''', (usuario_id,))
        
        return [row["nome"] for row in cursor.fetchall()]

def usuario_tem_funcao(usuario_id: int, funcao: str) -> bool:
    """
    Verifica se um usuário tem uma determinada função.
    
    Args:
        usuario_id: ID do usuário.
        funcao: Nome da função.
        
    Returns:
        bool: True se o usuário tem a função, False caso contrário.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT 1
        FROM usuario_funcoes uf
        JOIN funcoes f ON uf.funcao_id = f.id
        WHERE uf.usuario_id = ? AND f.nome = ?
        ''', (usuario_id, funcao))
        
        return cursor.fetchone() is not None

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
        
        # Encerra todas as sessões ativas do usuário
        cursor.execute('UPDATE sessoes SET ativa = 0 WHERE usuario_id = ?', (redefinicao["usuario_id"],))
        
        conn.commit()
        
        return True
