"""
Módulo de autenticação e autorização.
Este módulo contém funções para gerenciar usuários, autenticação e controle de acesso.
"""

import sqlite3
import hashlib
import secrets
import time
import jwt
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager

# Importa a função get_db do módulo database
from database import get_db

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
            ativo INTEGER NOT NULL DEFAULT 1
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
        'sub': usuario_id,
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

def verificar_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT revogado, data_expiracao
            FROM tokens
            WHERE token = ?
            ''', (token,))
            token_db = cursor.fetchone()
            
            if not token_db or token_db['revogado']:
                return None
        
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        
        if 'exp' in payload and payload['exp'] < time.time():
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE tokens SET revogado = 1 WHERE token = ?', (token,))
                conn.commit()
            return None
        
        return payload
    
    except jwt.PyJWTError:
        return None

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

def inicializar_autenticacao() -> None:
    """
    Inicializa o sistema de autenticação.
    Cria as tabelas necessárias e insere dados iniciais.
    """
    criar_tabelas_autenticacao()
    
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