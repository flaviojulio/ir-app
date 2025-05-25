import pytest
from fastapi.testclient import TestClient
import os
import json
from typing import Dict, Any, Generator

# Patch database path before other imports
TEST_DB_FILENAME = "acoes_ir_test.db"

# It's crucial to patch the DATABASE_FILE *before* importing main, database, or auth
# as they might use it at import time.
# We will use monkeypatch in fixtures for this.

# Now import application modules
from main import app
import database as db_module  # Use an alias to avoid confusion with local 'database' variables
import auth as auth_module # Use an alias

USER_COUNT = 0

@pytest.fixture(scope="session", autouse=True)
def setup_test_database(monkeypatch_session):
    """
    Session-scoped fixture to:
    1. Patch the DATABASE_FILE constant in the database module.
    2. Remove any old test database file.
    3. Create database tables and initial auth data.
    4. Yield for tests to run.
    5. Clean up the test database file after all tests in the session.
    """
    monkeypatch_session.setattr(db_module, "DATABASE_FILE", TEST_DB_FILENAME)
    
    if os.path.exists(TEST_DB_FILENAME):
        os.remove(TEST_DB_FILENAME)

    # Now that DATABASE_FILE is patched, these will use the test DB
    db_module.criar_tabelas()
    auth_module.inicializar_autenticacao() # Creates roles and default admin

    yield

    if os.path.exists(TEST_DB_FILENAME):
        os.remove(TEST_DB_FILENAME)


@pytest.fixture
def client(setup_test_database) -> Generator[TestClient, None, None]:
    """
    Provides a TestClient instance for making API requests.
    This fixture depends on setup_test_database to ensure the DB is ready.
    It also cleans relevant user-specific tables before each test.
    """
    # For most tests, we want to clear user-specific data, but not users/roles themselves
    # as creating users/roles for every test can be slow.
    # The `limpar_banco_dados` function in database.py clears ALL data including users/roles
    # which might be too much for every test. Let's create a more granular clear or accept
    # that users persist between tests within a session if not explicitly deleted.
    # For now, let's not aggressively clear users, but specific test data if needed.
    
    # A simple way to ensure some level of isolation if tests add data to global tables
    # without user_id scoping (which shouldn't happen with the new changes, but as a safeguard):
    # db_module.limpar_banco_dados() # This clears users too, so be careful.
    # auth_module.inicializar_autenticacao() # Re-initialize admin/roles if cleared.
    
    # Let's use a more targeted cleanup for operations, results, carteira for non-admin tests
    # This is tricky because we don't have usuario_id here easily.
    # The best approach is that tests clean up after themselves or operate on unique users.

    with TestClient(app) as c:
        yield c

# Helper to get unique user details
def get_unique_user_payload(username_prefix="testuser", email_prefix="test"):
    global USER_COUNT
    USER_COUNT += 1
    return {
        "username": f"{username_prefix}{USER_COUNT}",
        "email": f"{email_prefix}{USER_COUNT}@example.com",
        "senha": "password123",
        "nome_completo": f"Test User {USER_COUNT}"
    }

@pytest.fixture
def registered_user(client: TestClient) -> Dict[str, Any]:
    """Registers a new unique user and returns their creation data."""
    user_payload = get_unique_user_payload()
    response = client.post("/api/auth/registrar", json=user_payload)
    assert response.status_code == 200, f"Failed to register user: {response.json()}"
    # The response from /registrar is the user data including their ID
    registered_data = response.json()
    # Add plain password to the dict for login convenience in other fixtures/tests
    registered_data['plain_senha'] = user_payload['senha']
    return registered_data

@pytest.fixture
def auth_token(client: TestClient, registered_user: Dict[str, Any]) -> str:
    """Registers a user and returns an authentication token for them."""
    login_payload = {
        "username": registered_user["username"],
        "password": registered_user["plain_senha"] 
    }
    response = client.post("/api/auth/login", data=login_payload)
    assert response.status_code == 200, f"Failed to login: {response.json()}"
    token_data = response.json()
    return token_data["access_token"]

# --- Test Cases Start Here ---

def test_health_check(client: TestClient):
    """Test that the app is running and root path is accessible (if one exists)."""
    # Assuming your app doesn't have a root endpoint, let's check a known one like /docs
    response = client.get("/docs") 
    assert response.status_code == 200

# --- Authentication API Tests ---

def test_registrar_usuario_success(client: TestClient):
    user_payload = get_unique_user_payload(username_prefix="reg_success")
    response = client.post("/api/auth/registrar", json=user_payload)
    assert response.status_code == 200
    data = response.json()
    assert data["username"] == user_payload["username"]
    assert data["email"] == user_payload["email"]
    assert "id" in data
    assert "funcoes" in data and "usuario" in data["funcoes"]

def test_registrar_usuario_existing_username(client: TestClient, registered_user: Dict[str, Any]):
    # registered_user fixture already created a user. Try to register with same username.
    user_payload_fail = {
        "username": registered_user["username"], # Existing username
        "email": "newemail@example.com",
        "senha": "password123",
        "nome_completo": "Another User"
    }
    response = client.post("/api/auth/registrar", json=user_payload_fail)
    assert response.status_code == 400
    assert f"Username '{registered_user['username']}' já está em uso" in response.json()["detail"]

def test_registrar_usuario_existing_email(client: TestClient, registered_user: Dict[str, Any]):
    user_payload_fail = {
        "username": "newusername",
        "email": registered_user["email"], # Existing email
        "senha": "password123",
        "nome_completo": "Another User"
    }
    response = client.post("/api/auth/registrar", json=user_payload_fail)
    assert response.status_code == 400
    assert f"Email '{registered_user['email']}' já está em uso" in response.json()["detail"]

def test_login_success(client: TestClient, registered_user: Dict[str, Any]):
    login_payload = {
        "username": registered_user["username"],
        "password": registered_user["plain_senha"]
    }
    response = client.post("/api/auth/login", data=login_payload)
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"

def test_login_invalid_username(client: TestClient, registered_user: Dict[str, Any]):
    login_payload = {
        "username": "wronguser",
        "password": registered_user["plain_senha"]
    }
    response = client.post("/api/auth/login", data=login_payload)
    assert response.status_code == 401
    assert response.json()["detail"] == "Usuário ou senha incorretos"

def test_login_invalid_password(client: TestClient, registered_user: Dict[str, Any]):
    login_payload = {
        "username": registered_user["username"],
        "password": "wrongpassword"
    }
    response = client.post("/api/auth/login", data=login_payload)
    assert response.status_code == 401
    assert response.json()["detail"] == "Usuário ou senha incorretos"

def test_get_me_success(client: TestClient, auth_token: str, registered_user: Dict[str, Any]):
    headers = {"Authorization": f"Bearer {auth_token}"}
    response = client.get("/api/auth/me", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["username"] == registered_user["username"]
    assert data["email"] == registered_user["email"]
    assert data["id"] == registered_user["id"]

def test_get_me_no_token(client: TestClient):
    response = client.get("/api/auth/me")
    assert response.status_code == 401 # FastAPI's Depends(oauth2_scheme) handles this
    assert response.json()["detail"] == "Not authenticated" # Default message from FastAPI

def test_get_me_invalid_token(client: TestClient):
    headers = {"Authorization": "Bearer invalidtoken"}
    response = client.get("/api/auth/me", headers=headers)
    assert response.status_code == 401
    data = response.json()
    assert data["detail"]["message"] == "O token de autenticação é inválido ou malformado."
    assert data["detail"]["error_code"] == "TOKEN_INVALID"

def test_logout_success_and_token_revocation(client: TestClient, auth_token: str):
    headers = {"Authorization": f"Bearer {auth_token}"}
    
    # First, verify token is valid
    response_me_before_logout = client.get("/api/auth/me", headers=headers)
    assert response_me_before_logout.status_code == 200
    
    # Logout
    response_logout = client.post("/api/auth/logout", headers=headers)
    assert response_logout.status_code == 200
    assert response_logout.json() == {"mensagem": "Sessão encerrada com sucesso"}
    
    # Try to use the token again
    response_me_after_logout = client.get("/api/auth/me", headers=headers)
    assert response_me_after_logout.status_code == 401
    data = response_me_after_logout.json()
    assert data["detail"]["message"] == "O token de autenticação foi revogado (ex: logout ou alteração de senha)."
    assert data["detail"]["error_code"] == "TOKEN_REVOKED"

# --- Protected API Tests (Data Scoping) ---

@pytest.fixture
def registered_user_2(client: TestClient) -> Dict[str, Any]:
    """Registers a second unique user."""
    user_payload = get_unique_user_payload(username_prefix="user2_")
    response = client.post("/api/auth/registrar", json=user_payload)
    assert response.status_code == 200
    registered_data = response.json()
    registered_data['plain_senha'] = user_payload['senha']
    return registered_data

@pytest.fixture
def auth_token_user_2(client: TestClient, registered_user_2: Dict[str, Any]) -> str:
    """Gets auth token for the second user."""
    login_payload = {
        "username": registered_user_2["username"],
        "password": registered_user_2["plain_senha"]
    }
    response = client.post("/api/auth/login", data=login_payload)
    assert response.status_code == 200
    return response.json()["access_token"]


def test_operacoes_data_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1 creates an operation
    op_payload_user1 = {
        "date": "2023-01-01", "ticker": "PETR4", "operation": "buy",
        "quantity": 100, "price": 28.50, "fees": 5.20
    }
    response_create_user1 = client.post("/api/operacoes", json=op_payload_user1, headers=headers_user1)
    assert response_create_user1.status_code == 200
    assert response_create_user1.json() == {"mensagem": "Operação criada com sucesso."}

    # User 1 lists operations
    response_list_user1 = client.get("/api/operacoes", headers=headers_user1)
    assert response_list_user1.status_code == 200
    ops_user1 = response_list_user1.json()
    assert len(ops_user1) == 1
    assert ops_user1[0]["ticker"] == "PETR4"
    assert ops_user1[0]["usuario_id"] == registered_user["id"] 
    op_id_user1 = ops_user1[0]["id"]

    # User 2 lists operations - should be empty or not contain User 1's op
    response_list_user2 = client.get("/api/operacoes", headers=headers_user2)
    assert response_list_user2.status_code == 200
    ops_user2 = response_list_user2.json()
    assert len(ops_user2) == 0 # Assuming User 2 has no operations yet

    # User 2 creates an operation
    op_payload_user2 = {
        "date": "2023-01-02", "ticker": "VALE3", "operation": "buy",
        "quantity": 50, "price": 70.00, "fees": 3.10
    }
    response_create_user2 = client.post("/api/operacoes", json=op_payload_user2, headers=headers_user2)
    assert response_create_user2.status_code == 200

    # User 2 lists operations - should see their own
    response_list_user2_after = client.get("/api/operacoes", headers=headers_user2)
    assert response_list_user2_after.status_code == 200
    ops_user2_after = response_list_user2_after.json()
    assert len(ops_user2_after) == 1
    assert ops_user2_after[0]["ticker"] == "VALE3"
    assert ops_user2_after[0]["usuario_id"] == registered_user_2["id"]
    op_id_user2 = ops_user2_after[0]["id"]

    # User 1 lists operations again - should still only see their own
    response_list_user1_again = client.get("/api/operacoes", headers=headers_user1)
    assert response_list_user1_again.status_code == 200
    ops_user1_again = response_list_user1_again.json()
    assert len(ops_user1_again) == 1
    assert ops_user1_again[0]["ticker"] == "PETR4"
    
    # User 1 attempts to delete User 2's operation
    response_delete_attempt = client.delete(f"/api/operacoes/{op_id_user2}", headers=headers_user1)
    # database.remover_operacao is strict on (id, usuario_id), so it won't find it for user 1
    assert response_delete_attempt.status_code == 404 
    assert f"Operação {op_id_user2} não encontrada" in response_delete_attempt.json()["detail"]

    # User 2 successfully deletes their own operation
    response_delete_user2_own = client.delete(f"/api/operacoes/{op_id_user2}", headers=headers_user2)
    assert response_delete_user2_own.status_code == 200
    assert response_delete_user2_own.json()["mensagem"] == f"Operação {op_id_user2} removida com sucesso."


def test_upload_operacoes_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    
    operacoes_data = [
      {
        "date": "2023-02-01", "ticker": "BBAS3", "operation": "buy",
        "quantity": 200, "price": 40.00, "fees": 6.00
      },
      {
        "date": "2023-02-05", "ticker": "BBAS3", "operation": "sell",
        "quantity": 100, "price": 42.00, "fees": 5.50
      }
    ]
    
    with open("test_ops.json", "w") as f:
        json.dump(operacoes_data, f)

    with open("test_ops.json", "rb") as f_rb:
        response_upload = client.post("/api/upload", files={"file": ("test_ops.json", f_rb, "application/json")}, headers=headers_user1)
    
    os.remove("test_ops.json") # Clean up the temp file

    assert response_upload.status_code == 200
    assert f"Arquivo processado com sucesso. {len(operacoes_data)} operações importadas." in response_upload.json()["mensagem"]

    # Verify operations are associated with User 1
    response_list_user1 = client.get("/api/operacoes", headers=headers_user1)
    assert response_list_user1.status_code == 200
    ops_user1 = response_list_user1.json()
    
    # Check if the uploaded operations are present and associated with the user
    # This assumes no other operations were created by this user in this test context before upload
    # For more robustness, filter by ticker or check count increase.
    assert len(ops_user1) >= len(operacoes_data) 
    
    found_bbas3_buy = any(op["ticker"] == "BBAS3" and op["operation"] == "buy" and op["usuario_id"] == registered_user["id"] for op in ops_user1)
    found_bbas3_sell = any(op["ticker"] == "BBAS3" and op["operation"] == "sell" and op["usuario_id"] == registered_user["id"] for op in ops_user1)
    assert found_bbas3_buy
    assert found_bbas3_sell

def test_carteira_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1 creates an operation (this will trigger recalcular_carteira)
    op_payload_user1 = {
        "date": "2023-03-01", "ticker": "ITSA4", "operation": "buy",
        "quantity": 100, "price": 10.00, "fees": 1.00 
    } # Custo total = 100*10 + 1 = 1001. Preço médio = 10.01
    client.post("/api/operacoes", json=op_payload_user1, headers=headers_user1)

    # User 1 checks carteira
    response_carteira_user1 = client.get("/api/carteira", headers=headers_user1)
    assert response_carteira_user1.status_code == 200
    carteira_user1 = response_carteira_user1.json()
    assert len(carteira_user1) > 0 # Should have ITSA4
    itsa4_user1 = next((item for item in carteira_user1 if item["ticker"] == "ITSA4"), None)
    assert itsa4_user1 is not None
    assert itsa4_user1["quantidade"] == 100
    assert itsa4_user1["preco_medio"] == pytest.approx(10.01) 
    assert itsa4_user1["usuario_id"] == registered_user["id"]

    # User 2 checks carteira - should be empty or reflect their own data (empty for now)
    response_carteira_user2 = client.get("/api/carteira", headers=headers_user2)
    assert response_carteira_user2.status_code == 200
    carteira_user2 = response_carteira_user2.json()
    assert len(carteira_user2) == 0 # User 2 has no operations that form a carteira yet

    # User 2 creates an operation
    op_payload_user2 = {
        "date": "2023-03-02", "ticker": "MGLU3", "operation": "buy",
        "quantity": 200, "price": 3.00, "fees": 0.50
    } # Custo total = 200*3 + 0.5 = 600.5. Preço médio = 3.0025
    client.post("/api/operacoes", json=op_payload_user2, headers=headers_user2)

    # User 2 checks carteira again
    response_carteira_user2_after = client.get("/api/carteira", headers=headers_user2)
    assert response_carteira_user2_after.status_code == 200
    carteira_user2_after = response_carteira_user2_after.json()
    assert len(carteira_user2_after) > 0
    mglu3_user2 = next((item for item in carteira_user2_after if item["ticker"] == "MGLU3"), None)
    assert mglu3_user2 is not None
    assert mglu3_user2["quantidade"] == 200
    assert mglu3_user2["preco_medio"] == pytest.approx(3.0025)
    assert mglu3_user2["usuario_id"] == registered_user_2["id"]

    # User 1 checks carteira again - should be unchanged by User 2's actions
    response_carteira_user1_again = client.get("/api/carteira", headers=headers_user1)
    assert response_carteira_user1_again.status_code == 200
    carteira_user1_again = response_carteira_user1_again.json()
    itsa4_user1_again = next((item for item in carteira_user1_again if item["ticker"] == "ITSA4"), None)
    assert itsa4_user1_again is not None
    assert itsa4_user1_again["quantidade"] == 100
    assert itsa4_user1_again["preco_medio"] == pytest.approx(10.01)

def test_resultados_mensais_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1: Create a buy and a sell operation in the same month to generate a result
    op1_user1 = {"date": "2023-04-01", "ticker": "ABEV3", "operation": "buy", "quantity": 100, "price": 14.00, "fees": 1.00}
    op2_user1 = {"date": "2023-04-10", "ticker": "ABEV3", "operation": "sell", "quantity": 50, "price": 15.00, "fees": 0.50}
    client.post("/api/operacoes", json=op1_user1, headers=headers_user1)
    client.post("/api/operacoes", json=op2_user1, headers=headers_user1) # This triggers recalcular_resultados

    # User 1 checks resultados
    response_resultados_user1 = client.get("/api/resultados", headers=headers_user1)
    assert response_resultados_user1.status_code == 200
    resultados_user1 = response_resultados_user1.json()
    assert len(resultados_user1) > 0 # Should have a result for 2023-04
    res_abril_user1 = next((r for r in resultados_user1 if r["mes"] == "2023-04"), None)
    assert res_abril_user1 is not None
    assert res_abril_user1["usuario_id"] == registered_user["id"]

    # User 2 checks resultados - should be empty or not contain User 1's results
    response_resultados_user2 = client.get("/api/resultados", headers=headers_user2)
    assert response_resultados_user2.status_code == 200
    resultados_user2 = response_resultados_user2.json()
    res_abril_user2 = next((r for r in resultados_user2 if r["mes"] == "2023-04"), None)
    assert res_abril_user2 is None # User 2 should not see User 1's results

    # User 2: Create operations
    op1_user2 = {"date": "2023-04-05", "ticker": "BBDC4", "operation": "buy", "quantity": 200, "price": 20.00, "fees": 2.00}
    op2_user2 = {"date": "2023-04-15", "ticker": "BBDC4", "operation": "sell", "quantity": 100, "price": 22.00, "fees": 1.00}
    client.post("/api/operacoes", json=op1_user2, headers=headers_user2)
    client.post("/api/operacoes", json=op2_user2, headers=headers_user2)

    # User 2 checks resultados again
    response_resultados_user2_after = client.get("/api/resultados", headers=headers_user2)
    assert response_resultados_user2_after.status_code == 200
    resultados_user2_after = response_resultados_user2_after.json()
    res_abril_user2_after = next((r for r in resultados_user2_after if r["mes"] == "2023-04"), None)
    assert res_abril_user2_after is not None
    assert res_abril_user2_after["usuario_id"] == registered_user_2["id"]
    # Ensure User 1's specific ticker data isn't mixed in (though mes is the primary check here for results)
    # This would be more about checking the calculation logic if we had exact values.

    # User 1 checks resultados again - should be unchanged by User 2's actions
    response_resultados_user1_again = client.get("/api/resultados", headers=headers_user1)
    assert response_resultados_user1_again.status_code == 200
    resultados_user1_again = response_resultados_user1_again.json()
    res_abril_user1_again = next((r for r in resultados_user1_again if r["mes"] == "2023-04"), None)
    assert res_abril_user1_again is not None
    assert res_abril_user1_again["ganho_liquido_swing"] == pytest.approx(48.5) # (50*15 - 0.5) - (50*14.01) approx. (749.5 - 700.5) = 49. Price_medio = (100*14+1)/100 = 14.01


def test_operacoes_fechadas_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1: Create a buy and a sell operation for the same ticker
    op_buy_u1 = {"date": "2023-05-01", "ticker": "PETZ3", "operation": "buy", "quantity": 100, "price": 5.00, "fees": 0.50}
    op_sell_u1 = {"date": "2023-05-10", "ticker": "PETZ3", "operation": "sell", "quantity": 100, "price": 6.00, "fees": 0.60}
    client.post("/api/operacoes", json=op_buy_u1, headers=headers_user1)
    client.post("/api/operacoes", json=op_sell_u1, headers=headers_user1) # Triggers recalculate, which includes calcular_operacoes_fechadas

    # User 1 checks operacoes fechadas
    response_fechadas_user1 = client.get("/api/operacoes/fechadas", headers=headers_user1)
    assert response_fechadas_user1.status_code == 200
    fechadas_user1 = response_fechadas_user1.json()
    assert len(fechadas_user1) > 0
    op_petz3_u1 = next((op for op in fechadas_user1 if op["ticker"] == "PETZ3"), None)
    assert op_petz3_u1 is not None
    assert op_petz3_u1["quantidade"] == 100
    # Check if usuario_id is present in the saved operacao_fechada (it should be, from database.py)
    # The response model OperacaoFechada in models.py might need 'usuario_id' if we want to assert it here.
    # For now, scoping is implicitly tested by User 2 not seeing this.

    # User 2 checks operacoes fechadas - should be empty
    response_fechadas_user2 = client.get("/api/operacoes/fechadas", headers=headers_user2)
    assert response_fechadas_user2.status_code == 200
    fechadas_user2 = response_fechadas_user2.json()
    assert len(fechadas_user2) == 0

    # User 2: Create their own closed operation
    op_buy_u2 = {"date": "2023-05-02", "ticker": "WEGE3", "operation": "buy", "quantity": 50, "price": 30.00, "fees": 0.25}
    op_sell_u2 = {"date": "2023-05-12", "ticker": "WEGE3", "operation": "sell", "quantity": 50, "price": 35.00, "fees": 0.30}
    client.post("/api/operacoes", json=op_buy_u2, headers=headers_user2)
    client.post("/api/operacoes", json=op_sell_u2, headers=headers_user2)

    # User 2 checks operacoes fechadas again
    response_fechadas_user2_after = client.get("/api/operacoes/fechadas", headers=headers_user2)
    assert response_fechadas_user2_after.status_code == 200
    fechadas_user2_after = response_fechadas_user2_after.json()
    assert len(fechadas_user2_after) > 0
    op_wege3_u2 = next((op for op in fechadas_user2_after if op["ticker"] == "WEGE3"), None)
    assert op_wege3_u2 is not None
    assert op_wege3_u2["quantidade"] == 50

    # User 1 checks operacoes fechadas again - should only see their PETZ3 op
    response_fechadas_user1_again = client.get("/api/operacoes/fechadas", headers=headers_user1)
    assert response_fechadas_user1_again.status_code == 200
    fechadas_user1_again = response_fechadas_user1_again.json()
    assert len(fechadas_user1_again) == 1
    assert fechadas_user1_again[0]["ticker"] == "PETZ3"

def test_darfs_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1: Create operations that would generate a DARF (significant profit in a month)
    # For DARF to be generated, ir_pagar_total_mes >= 10
    # Ganho líquido Day Trade * 0.20 >= 10  => Ganho líquido Day Trade >= 50
    # (Venda - Compra - Taxas) >= 50
    op_buy_u1_dt = {"date": "2023-06-01", "ticker": "DAYT1", "operation": "buy", "quantity": 100, "price": 10.00, "fees": 1.00} # Custo = 1001
    op_sell_u1_dt = {"date": "2023-06-01", "ticker": "DAYT1", "operation": "sell", "quantity": 100, "price": 16.00, "fees": 1.00} # Venda = 1599. Lucro = 1599 - 1001 = 598. IR = 598 * 0.2 = 119.6
    client.post("/api/operacoes", json=op_buy_u1_dt, headers=headers_user1)
    client.post("/api/operacoes", json=op_sell_u1_dt, headers=headers_user1)

    # User 1 checks DARFs
    response_darfs_user1 = client.get("/api/darfs", headers=headers_user1)
    assert response_darfs_user1.status_code == 200
    darfs_user1 = response_darfs_user1.json()
    assert len(darfs_user1) > 0
    darf_junho_user1 = next((d for d in darfs_user1 if d["competencia"] == "2023-06"), None)
    assert darf_junho_user1 is not None
    assert darf_junho_user1["valor"] == pytest.approx(119.6) # IR Day Trade (20%) - IRRF (1% sobre venda de 1600 = 16). 119.6 - 16 = 103.6
                                                          # Recalculating based on _calcular_resultado_dia and recalcular_resultados:
                                                          # resultado_day["vendas"] = 1600 - 1 = 1599
                                                          # resultado_day["custo"] = 1000 + 1 = 1001
                                                          # resultado_day["ganho_liquido"] = 1599 - 1001 = 598
                                                          # resultado_day["irrf"] = 1600 * 0.01 = 16
                                                          # ir_devido_day = 598 * 0.20 = 119.6
                                                          # ir_pagar_total_mes = 119.6 (swing trade IR is 0) - 16 (irrf_day) = 103.6
    assert darf_junho_user1["valor"] == pytest.approx(103.6)


    # User 2 checks DARFs - should be empty
    response_darfs_user2 = client.get("/api/darfs", headers=headers_user2)
    assert response_darfs_user2.status_code == 200
    darfs_user2 = response_darfs_user2.json()
    assert len(darfs_user2) == 0

    # User 2: Create operations (no DARF expected or different DARF)
    op_buy_u2_st = {"date": "2023-06-05", "ticker": "SWNG2", "operation": "buy", "quantity": 100, "price": 20.00, "fees": 1.00} # Custo = 2001
    op_sell_u2_st = {"date": "2023-06-10", "ticker": "SWNG2", "operation": "sell", "quantity": 100, "price": 21.00, "fees": 1.00} # Venda = 2099. Lucro = 98. IR Swing = 98 * 0.15 = 14.7.
                                                                                                                            # (Assumindo vendas > 20k para não isenção, ou alterando para ser daytrade para forçar IR)
                                                                                                                            # Para simplificar, vamos fazer uma venda pequena que não gere DARF
    client.post("/api/operacoes", json=op_buy_u2_st, headers=headers_user2)
    client.post("/api/operacoes", json=op_sell_u2_st, headers=headers_user2)

    # User 2 checks DARFs again
    response_darfs_user2_after = client.get("/api/darfs", headers=headers_user2)
    assert response_darfs_user2_after.status_code == 200
    darfs_user2_after = response_darfs_user2_after.json()
    # Swing trade com lucro 98, IR 14.7. Se vendas < 20k, isento. Se vendas > 20k, IR devido.
    # A lógica de isenção é resultado_mes_swing["vendas"] <= 20000.0. Vendas aqui é 2100 - 1 = 2099. Logo, isento.
    # Portanto, não deve gerar DARF para User 2.
    assert len(darfs_user2_after) == 0 


    # User 1 checks DARFs again - should be unchanged
    response_darfs_user1_again = client.get("/api/darfs", headers=headers_user1)
    assert response_darfs_user1_again.status_code == 200
    darfs_user1_again = response_darfs_user1_again.json()
    assert len(darfs_user1_again) == 1
    darf_junho_user1_again = next((d for d in darfs_user1_again if d["competencia"] == "2023-06"), None)
    assert darf_junho_user1_again is not None
    assert darf_junho_user1_again["valor"] == pytest.approx(103.6)

def test_operacoes_fechadas_resumo_scoping(client: TestClient, auth_token: str, registered_user: Dict[str, Any], auth_token_user_2: str, registered_user_2: Dict[str, Any]):
    headers_user1 = {"Authorization": f"Bearer {auth_token}"}
    headers_user2 = {"Authorization": f"Bearer {auth_token_user_2}"}

    # User 1: Create a closed operation (reuse from operacoes_fechadas test for simplicity in setup)
    op_buy_u1 = {"date": "2023-07-01", "ticker": "RSUM1", "operation": "buy", "quantity": 100, "price": 10.00, "fees": 1.00} # Custo 1001
    op_sell_u1 = {"date": "2023-07-10", "ticker": "RSUM1", "operation": "sell", "quantity": 100, "price": 12.00, "fees": 1.00} # Venda 1199. Resultado = 1199 - 1001 = 198
    client.post("/api/operacoes", json=op_buy_u1, headers=headers_user1)
    client.post("/api/operacoes", json=op_sell_u1, headers=headers_user1)

    # User 1 checks resumo
    response_resumo_user1 = client.get("/api/operacoes/fechadas/resumo", headers=headers_user1)
    assert response_resumo_user1.status_code == 200
    resumo_user1 = response_resumo_user1.json()
    assert resumo_user1["total_operacoes"] >= 1 # Can be more if other tests for user1 ran before
    assert resumo_user1["resumo_por_ticker"]["RSUM1"]["lucro_total"] == pytest.approx(198)

    # User 2 checks resumo - should be empty or reflect their own data
    response_resumo_user2 = client.get("/api/operacoes/fechadas/resumo", headers=headers_user2)
    assert response_resumo_user2.status_code == 200
    resumo_user2 = response_resumo_user2.json()
    assert "RSUM1" not in resumo_user2.get("resumo_por_ticker", {})

    # User 2: Create their own closed operation
    op_buy_u2 = {"date": "2023-07-02", "ticker": "RSUM2", "operation": "buy", "quantity": 50, "price": 20.00, "fees": 1.00} # Custo 1001
    op_sell_u2 = {"date": "2023-07-12", "ticker": "RSUM2", "operation": "sell", "quantity": 50, "price": 18.00, "fees": 1.00} # Venda 899. Resultado = 899 - 1001 = -102
    client.post("/api/operacoes", json=op_buy_u2, headers=headers_user2)
    client.post("/api/operacoes", json=op_sell_u2, headers=headers_user2)

    # User 2 checks resumo again
    response_resumo_user2_after = client.get("/api/operacoes/fechadas/resumo", headers=headers_user2)
    assert response_resumo_user2_after.status_code == 200
    resumo_user2_after = response_resumo_user2_after.json()
    assert resumo_user2_after["total_operacoes"] >= 1
    assert resumo_user2_after["resumo_por_ticker"]["RSUM2"]["lucro_total"] == pytest.approx(-102)

    # User 1 checks resumo again - should not include User 2's RSUM2
    response_resumo_user1_again = client.get("/api/operacoes/fechadas/resumo", headers=headers_user1)
    assert response_resumo_user1_again.status_code == 200
    resumo_user1_again = response_resumo_user1_again.json()
    assert "RSUM2" not in resumo_user1_again.get("resumo_por_ticker", {})
    assert resumo_user1_again["resumo_por_ticker"]["RSUM1"]["lucro_total"] == pytest.approx(198)


# TODO: Add tests for expired tokens if feasible without overcomplicating.
# This might require mocking time or auth.verificar_token's internals.

# TODO: Add tests for other protected endpoints following the same data scoping pattern:
# (No more remaining from the list, good)

# TODO: Add tests for admin functionalities if time permits,
# ensuring only admin can access them. The `get_admin_user` fixture would need
# to be used, and an admin user created (e.g. by promoting a user or using default admin).
# The default admin is created by `inicializar_autenticacao`, username 'admin', pass 'admin123'.

# Note on `limpar_banco_dados` in client fixture:
# If tests become flaky due to shared user data, more aggressive cleanup will be needed.
# For now, relying on unique user creation for most tests should provide isolation.
# The `setup_test_database` fixture ensures tables are created once per session.
# `database.limpar_banco_dados()` could be called in `client` fixture before yielding,
# and then `auth_module.inicializar_autenticacao()` called again to recreate admin/roles
# if tests need a completely fresh state including users.
# However, this would slow down tests. The current approach is a balance.
# A better approach for cleaning specific user data without affecting auth tables is needed
# if we want tests to clean up perfectly without slowing down user/role creation.
# The `database.limpar_banco_dados_usuario(usuario_id)` can be used in specific tests.
# For example, in `test_operacoes_data_scoping` after all assertions for user1, we could call
# client.delete(f"/api/usuarios/{registered_user['id']}", headers=admin_auth_headers)
# or directly use `limpar_banco_dados_usuario(registered_user['id'])` if we have DB access in tests.
# But this requires admin rights or direct DB manipulation.

# For now, the tests focus on creating new, unique users for each scenario where isolation is critical.
# The `operacoes` tests do clean up their created operations.
# The `upload` and `carteira` tests build on potentially existing state for that user from other tests,
# which is generally okay if the user is unique to that test function or fixture chain.
# The use of `get_unique_user_payload` and `registered_user` (function-scoped) helps.
```
