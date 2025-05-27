from fastapi import FastAPI, UploadFile, File, HTTPException, Path, Body, Depends, status
from fastapi.middleware.cors import CORSMiddleware
import json
from typing import List, Dict, Any
import uvicorn

from auth import TokenExpiredError, InvalidTokenError, TokenNotFoundError, TokenRevokedError

from models import (
    OperacaoCreate, Operacao, ResultadoMensal, CarteiraAtual, 
    DARF, AtualizacaoCarteira, OperacaoFechada,
    # Modelos de autenticação
    UsuarioCreate, UsuarioUpdate, UsuarioResponse, LoginResponse, FuncaoCreate, FuncaoUpdate, FuncaoResponse, TokenResponse
)

from database import (
    criar_tabelas, 
    limpar_banco_dados, 
    # get_db, remover_operacao, obter_todas_operacoes removed
)

import services # Keep this for other service functions
from services import (
    calcular_operacoes_fechadas,
    processar_operacoes,
    calcular_resultados_mensais,
    calcular_carteira_atual,
    gerar_darfs,
    inserir_operacao_manual,
    atualizar_item_carteira,
    # recalcular_carteira, recalcular_resultados are internal to services now for delete
    # Add new service imports
    listar_operacoes_service,
    deletar_operacao_service,
    obter_operacao_service, # Added for returning created operacao
    gerar_resumo_operacoes_fechadas # Added for summary
)

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import auth # Keep this for other auth functions
# auth.get_db removed from here

# Inicialização do banco de dados
criar_tabelas() # Creates non-auth tables
auth.inicializar_autenticacao() # Initializes authentication system (creates auth tables, modifies others, adds admin)

app = FastAPI(
    title="API de Acompanhamento de Carteiras de Ações e IR",
    description="API para upload de operações de ações e cálculo de imposto de renda",
    version="1.0.0"
)

# Configuração de CORS para permitir requisições de origens diferentes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuração do OAuth2
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Função para obter o usuário atual
async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    try:
        payload = auth.verificar_token(token)
    except TokenExpiredError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "O token de autenticação expirou.", "error_code": "TOKEN_EXPIRED"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    except InvalidTokenError as e: # Use 'as e' to include original error message
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": f"O token de autenticação é inválido ou malformado: {str(e)}", "error_code": "TOKEN_INVALID"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    except TokenNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "O token de autenticação não foi reconhecido.", "error_code": "TOKEN_NOT_FOUND"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    except TokenRevokedError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"message": "O token de autenticação foi revogado (ex: logout ou alteração de senha).", "error_code": "TOKEN_REVOKED"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e: # Capture and potentially log the original exception
        # Log the exception e for debugging (e.g., import logging; logging.exception("Unexpected error"))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail={"message": f"Erro inesperado durante a verificação do token: {str(e)}", "error_code": "UNEXPECTED_TOKEN_VERIFICATION_ERROR"},
        )

    sub_str = payload.get("sub")
    if not sub_str: 
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail={"message": "Token inválido: ID de usuário (sub) ausente no payload.", "error_code": "TOKEN_PAYLOAD_MISSING_SUB"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        usuario_id = int(sub_str) # Converte para int
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail={"message": "Token inválido: ID de usuário (sub) não é um inteiro válido.", "error_code": "TOKEN_PAYLOAD_INVALID_SUB_FORMAT"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Agora usuario_id é um int e pode ser usado para chamar auth.obter_usuario
    usuario_data = auth.obter_usuario(usuario_id) 
    if not usuario_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, # Ou status.HTTP_404_NOT_FOUND
            detail={"message": "Usuário associado ao token não encontrado.", "error_code": "USER_FOR_TOKEN_NOT_FOUND"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return usuario_data

# Função para verificar se o usuário é administrador
async def get_admin_user(usuario: Dict = Depends(get_current_user)) -> Dict:
    if "admin" not in usuario.get("funcoes", []):
        raise HTTPException(
            status_code=403,
            detail="Acesso negado. Permissão de administrador necessária.",
        )
    return usuario

# Endpoints de autenticação
@app.post("/api/auth/registrar", response_model=UsuarioResponse)
async def registrar_usuario(usuario: UsuarioCreate):
    """
    Registra um novo usuário no sistema.
    """
    try:
        usuario_id = auth.criar_usuario(
            username=usuario.username,
            email=usuario.email,
            senha=usuario.senha,
            nome_completo=usuario.nome_completo
        )
        
        return auth.obter_usuario(usuario_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao registrar usuário: {str(e)}")


@app.post("/api/auth/login", response_model=TokenResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = auth.verificar_credenciais(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Usuário ou senha incorretos")
    token = auth.gerar_token(user["id"])
    return {"access_token": token, "token_type": "bearer"}

# Commented out /api/auth/me endpoint removed.

@app.post("/api/auth/logout")
async def logout(token: str = Depends(oauth2_scheme)):
    """
    Encerra a sessão do usuário revogando o token.
    """
    success = auth.revogar_token(token)
    
    if not success:
        raise HTTPException(status_code=400, detail="Erro ao encerrar sessão")
    
    return {"mensagem": "Sessão encerrada com sucesso"}

@app.get("/api/auth/me", response_model=UsuarioResponse)
async def get_me(usuario: Dict = Depends(get_current_user)):
    """
    Retorna os dados do usuário autenticado.
    """
    return usuario

# Endpoints de administração de usuários (apenas para administradores)
@app.get("/api/usuarios", response_model=List[UsuarioResponse])
async def listar_usuarios(admin: Dict = Depends(get_admin_user)):
    """
    Lista todos os usuários do sistema.
    Requer permissão de administrador.
    """
    return auth.obter_todos_usuarios()

@app.get("/api/usuarios/{usuario_id}", response_model=UsuarioResponse)
async def obter_usuario_por_id(
    usuario_id: int = Path(..., description="ID do usuário"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Obtém os dados de um usuário pelo ID.
    Requer permissão de administrador.
    """
    usuario = auth.obter_usuario(usuario_id)
    
    if not usuario:
        raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado")
    
    return usuario

@app.put("/api/usuarios/{usuario_id}", response_model=UsuarioResponse)
async def atualizar_usuario_por_id(
    usuario_data: UsuarioUpdate,
    usuario_id: int = Path(..., description="ID do usuário"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Atualiza os dados de um usuário.
    Requer permissão de administrador.
    """
    try:
        success = auth.atualizar_usuario(usuario_id, usuario_data.model_dump(exclude_unset=True))
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado")
        
        return auth.obter_usuario(usuario_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar usuário: {str(e)}")

@app.delete("/api/usuarios/{usuario_id}")
async def excluir_usuario(
    usuario_id: int = Path(..., description="ID do usuário"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Exclui um usuário do sistema.
    Requer permissão de administrador.
    """
    success = auth.excluir_usuario(usuario_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado")
    
    return {"mensagem": f"Usuário {usuario_id} excluído com sucesso"}

@app.post("/api/usuarios/{usuario_id}/funcoes/{funcao_nome}")
async def adicionar_funcao_a_usuario(
    usuario_id: int = Path(..., description="ID do usuário"),
    funcao_nome: str = Path(..., description="Nome da função"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Adiciona uma função a um usuário.
    Requer permissão de administrador.
    """
    success = auth.adicionar_funcao_usuario(usuario_id, funcao_nome)
    
    if not success:
        raise HTTPException(status_code=404, detail="Usuário ou função não encontrados")
    
    updated_usuario = auth.obter_usuario(usuario_id)
    if not updated_usuario:
        # Should not happen if adicionar_funcao_usuario was successful and usuario_id is valid
        raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado após adicionar função.")
    return updated_usuario

@app.delete("/api/usuarios/{usuario_id}/funcoes/{funcao_nome}", response_model=UsuarioResponse)
async def remover_funcao_de_usuario(
    usuario_id: int = Path(..., description="ID do usuário"),
    funcao_nome: str = Path(..., description="Nome da função"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Remove uma função de um usuário.
    Requer permissão de administrador.
    """
    success = auth.remover_funcao_usuario(usuario_id, funcao_nome)
    
    if not success:
        # This could mean user not found, function not found, or user didn't have the function.
        # For simplicity, we'll check if the user exists to give a more specific 404 for the user.
        usuario = auth.obter_usuario(usuario_id)
        if not usuario:
            raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado.")
        # If user exists, the issue was with the function or its assignment.
        raise HTTPException(status_code=404, detail=f"Função '{funcao_nome}' não encontrada ou não associada ao usuário {usuario_id}.")

    updated_usuario = auth.obter_usuario(usuario_id)
    if not updated_usuario:
        # Should not happen if remover_funcao_usuario was successful and usuario_id is valid
        raise HTTPException(status_code=404, detail=f"Usuário {usuario_id} não encontrado após remover função.")
    return updated_usuario

# Endpoints para gerenciar funções
@app.get("/api/funcoes", response_model=List[FuncaoResponse])
async def listar_funcoes(admin: Dict = Depends(get_admin_user)):
    """
    Lista todas as funções do sistema.
    Requer permissão de administrador.
    """
    return auth.obter_todas_funcoes()

@app.post("/api/funcoes", response_model=FuncaoResponse)
async def criar_nova_funcao(
    funcao: FuncaoCreate,
    admin: Dict = Depends(get_admin_user)
):
    """
    Cria uma nova função no sistema.
    Requer permissão de administrador.
    """
    try:
        funcao_id = auth.criar_funcao(funcao.nome, funcao.descricao)
        
        # Obtém a função criada usando o novo serviço
        funcao_criada = auth.obter_funcao(funcao_id)
        if not funcao_criada:
            # This case should ideally not happen if criar_funcao succeeded
            raise HTTPException(status_code=500, detail="Erro ao obter função recém-criada.")
        return funcao_criada
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao criar função: {str(e)}")

@app.put("/api/funcoes/{funcao_id}", response_model=FuncaoResponse)
async def atualizar_funcao_existente(
    funcao_id: int = Path(..., description="ID da função a ser atualizada"),
    funcao_data: FuncaoUpdate = Body(...),
    admin: Dict = Depends(get_admin_user)
):
    """
    Atualiza uma função existente no sistema.
    Requer permissão de administrador.
    """
    try:
        # Verificar se pelo menos um campo foi enviado para atualização
        if funcao_data.model_dump(exclude_unset=True) == {}:
            raise HTTPException(status_code=400, detail="Pelo menos um campo (nome ou descrição) deve ser fornecido para atualização.")

        success = auth.atualizar_funcao(
            funcao_id,
            nome=funcao_data.nome,
            descricao=funcao_data.descricao
        )
        if not success:
            # Se atualizar_funcao retorna False, pode ser "não encontrado" ou outro motivo não coberto por ValueError
            # Verificar se a função realmente não existe mais pode ser redundante se auth.atualizar_funcao já lida com isso
            updated_funcao = auth.obter_funcao(funcao_id)
            if not updated_funcao:
                 raise HTTPException(status_code=404, detail=f"Função com ID {funcao_id} não encontrada.")
            # Se chegou aqui, a atualização falhou por um motivo não de "não encontrado" que não levantou ValueError
            # Isso pode indicar um problema lógico em auth.atualizar_funcao se não houver conflito de nome
            # Para agora, vamos assumir que o nome pode ser o problema se não for ValueError
            raise HTTPException(status_code=409, detail=f"Não foi possível atualizar a função com ID {funcao_id}. Verifique se o novo nome já está em uso.")


        updated_funcao = auth.obter_funcao(funcao_id)
        if not updated_funcao:
            # Este caso é improvável se success=True, mas é uma salvaguarda
            raise HTTPException(status_code=404, detail=f"Função com ID {funcao_id} não encontrada após a atualização.")
        
        return updated_funcao
    except ValueError as e: # Captura conflitos de nome ou nome vazio de auth.atualizar_funcao
        raise HTTPException(status_code=400, detail=str(e)) # Reutiliza 400 para conflito de nome/validação
    except HTTPException as e: # Re-raise HTTPExceptions para não mascará-las com 500
        raise e
    except Exception as e:
        # Log a exceção 'e' aqui para depuração
        raise HTTPException(status_code=500, detail=f"Erro interno ao atualizar função: {str(e)}")

@app.delete("/api/funcoes/{funcao_id}", response_model=Dict[str, str])
async def deletar_funcao_existente(
    funcao_id: int = Path(..., description="ID da função a ser excluída"),
    admin: Dict = Depends(get_admin_user)
):
    """
    Exclui uma função existente do sistema.
    A função não pode ser excluída se estiver atualmente em uso por algum usuário.
    Requer permissão de administrador.
    """
    try:
        success = auth.excluir_funcao(funcao_id)
        if not success:
            # Isso cobre o caso onde obter_funcao(funcao_id) em excluir_funcao retorna None
            raise HTTPException(status_code=404, detail=f"Função com ID {funcao_id} não encontrada.")
        
        return {"mensagem": f"Função {funcao_id} excluída com sucesso"}
    except ValueError as e: # Captura o erro de função em uso
        raise HTTPException(status_code=409, detail=str(e)) # 409 Conflict
    except HTTPException as e: # Re-raise outras HTTPExceptions
        raise e
    except Exception as e:
        # Log a exceção 'e' aqui para depuração
        raise HTTPException(status_code=500, detail=f"Erro interno ao excluir função: {str(e)}")

# Endpoints de operações com autenticação
@app.get("/api/operacoes", response_model=List[Operacao])
async def listar_operacoes(usuario: Dict[str, Any] = Depends(get_current_user)): # Type hint improved
    try:
        # Use the new service function
        operacoes = listar_operacoes_service(usuario_id=usuario["id"])
        return operacoes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar operações: {str(e)}")

@app.post("/api/upload", response_model=Dict[str, str])
async def upload_operacoes(
    file: UploadFile = File(...),
    usuario: Dict = Depends(get_current_user)
):
    """
    Endpoint para upload de arquivo JSON com operações de compra e venda de ações.
    
    O arquivo deve seguir o formato:
    [
      {
        "date": "YYYY-MM-DD",
        "ticker": "PETR4",
        "operation": "buy"|"sell",
        "quantity": 100,
        "price": 28.50,
        "fees": 5.20
      },
      …
    ]
    """
    try:
        # Lê o conteúdo do arquivo
        conteudo = await file.read()
        
        # Converte o JSON para uma lista de dicionários
        operacoes_json = json.loads(conteudo)
        
        # Valida e processa as operações
        operacoes = [OperacaoCreate(**op) for op in operacoes_json]
        
        # Salva as operações no banco de dados com o ID do usuário
        processar_operacoes(operacoes, usuario_id=usuario["id"])
        
        return {"mensagem": f"Arquivo processado com sucesso. {len(operacoes)} operações importadas."}
    
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Formato de arquivo JSON inválido")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")

@app.get("/api/resultados", response_model=List[ResultadoMensal])
async def obter_resultados(usuario: Dict = Depends(get_current_user)):
    """
    Retorna os resultados mensais de apuração de imposto de renda.
    """
    try:
        resultados = calcular_resultados_mensais(usuario_id=usuario["id"])
        return resultados
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular resultados: {str(e)}")

@app.get("/api/carteira", response_model=List[CarteiraAtual])
async def obter_carteira(usuario: Dict = Depends(get_current_user)):
    """
    Retorna a carteira atual de ações.
    """
    try:
        carteira = calcular_carteira_atual(usuario_id=usuario["id"])
        return carteira
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular carteira: {str(e)}")

@app.get("/api/darfs", response_model=List[DARF])
async def obter_darfs(usuario: Dict = Depends(get_current_user)):
    """
    Retorna os DARFs gerados para pagamento de imposto de renda.
    """
    try:
        darfs = gerar_darfs(usuario_id=usuario["id"])
        return darfs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar DARFs: {str(e)}")

# Novos endpoints para as funcionalidades adicionais

@app.post("/api/operacoes", response_model=Operacao)
async def criar_operacao(
    operacao: OperacaoCreate,
    usuario: Dict = Depends(get_current_user)
):
    """
    Cria uma nova operação manualmente e retorna a operação criada.
    
    Args:
        operacao: Dados da operação a ser criada.
    """
    try:
        new_operacao_id = services.inserir_operacao_manual(operacao, usuario_id=usuario["id"])
        operacao_criada = services.obter_operacao_service(new_operacao_id, usuario_id=usuario["id"])
        if not operacao_criada:
            # This case should ideally not happen if insertion and ID return were successful
            raise HTTPException(status_code=500, detail="Operação criada mas não pôde ser recuperada.")
        return operacao_criada
    except Exception as e:
        # Log the exception e for detailed debugging
        raise HTTPException(status_code=500, detail=f"Erro ao criar operação: {str(e)}")

@app.put("/api/carteira/{ticker}", response_model=Dict[str, str])
async def atualizar_carteira(
    ticker: str = Path(..., description="Ticker da ação"), 
    dados: AtualizacaoCarteira = Body(...),
    usuario: Dict = Depends(get_current_user)
):
    """
    Atualiza a quantidade e o preço médio de uma ação na carteira.
    O custo total será calculado automaticamente (quantidade * preço médio).
    
    Args:
        ticker: Ticker da ação a ser atualizada.
        dados: Novos dados da ação (quantidade e preço médio).
    """
    try:
        # Verifica se o ticker no path é o mesmo do body
        if ticker.upper() != dados.ticker.upper():
            raise HTTPException(status_code=400, detail="O ticker no path deve ser o mesmo do body")
        
        atualizar_item_carteira(dados, usuario_id=usuario["id"])
        return {"mensagem": f"Ação {ticker.upper()} atualizada com sucesso."}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar ação: {str(e)}")

@app.get("/api/operacoes/fechadas", response_model=List[OperacaoFechada])
async def obter_operacoes_fechadas(usuario: Dict = Depends(get_current_user)):
    """
    Retorna as operações fechadas (compra seguida de venda ou vice-versa).
    Inclui detalhes como data de abertura e fechamento, preços, quantidade e resultado.
    """
    try:
        operacoes_fechadas = calcular_operacoes_fechadas(usuario_id=usuario["id"])
        return operacoes_fechadas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular operações fechadas: {str(e)}")

@app.get("/api/operacoes/fechadas/resumo", response_model=Dict[str, Any])
async def obter_resumo_operacoes_fechadas(usuario: Dict = Depends(get_current_user)):
    """
    Retorna um resumo das operações fechadas, incluindo:
    - Total de operações fechadas
    - Lucro/prejuízo total
    - Lucro/prejuízo de operações day trade
    - Lucro/prejuízo de operações swing trade
    - Operações mais lucrativas
    - Operações com maior prejuízo
    """
    try:
        resumo = services.gerar_resumo_operacoes_fechadas(usuario_id=usuario["id"])
        return resumo
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular resumo de operações fechadas: {str(e)}")
    
@app.delete("/api/admin/reset-financial-data", response_model=Dict[str, str])
async def resetar_banco(admin: Dict = Depends(get_admin_user)):
    """
    Remove todos os dados financeiros e operacionais do banco de dados.
    Requer permissão de administrador.
    Preserva dados de usuários e autenticação.
    """
    try:
        limpar_banco_dados()
        return {"mensagem": "Dados financeiros e operacionais foram removidos com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao limpar banco de dados: {str(e)}")

@app.delete("/api/operacoes/{operacao_id}", response_model=Dict[str, str])
async def deletar_operacao(
    operacao_id: int = Path(..., description="ID da operação"),
    usuario: Dict = Depends(get_current_user)
):
    """
    Remove uma operação pelo ID.
    
    Args:
        operacao_id: ID da operação a ser removida.
    """
    try:
        # Use the new service function
        success = deletar_operacao_service(operacao_id=operacao_id, usuario_id=usuario["id"])
        if success:
            return {"mensagem": f"Operação {operacao_id} removida com sucesso."}
        else:
            # This case might be hit if remover_operacao returned False but didn't raise an error
            # that deletar_operacao_service would propagate.
            # database.remover_operacao returns bool, so service translates this.
            # If service returns False, it means op not found for that user.
            raise HTTPException(status_code=404, detail=f"Operação {operacao_id} não encontrada ou não pertence ao usuário.")
    except HTTPException as e:
        raise e # Re-raise HTTPExceptions directly
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao remover operação: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)