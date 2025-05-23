import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import status
import os
import sys
import json # For file uploads

# Add the parent directory to the sys.path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app # FastAPI app instance
from test_data import new_user_data, login_data, sample_operation_data, sample_operations_file_content, admin_login_data
from auth import criar_tabelas_autenticacao, get_db, adicionar_funcao_usuario, criar_usuario as auth_criar_usuario
from database import limpar_banco_dados, criar_tabelas as criar_app_tabelas

# Base URL for the API
BASE_URL = "http://localhost:8000/api"

@pytest.fixture(scope="function", autouse=True)
def setup_database_for_api_tests():
    # Ensure all tables (auth and app) are created and cleaned for each test
    criar_tabelas_autenticacao()
    criar_app_tabelas() # Ensure application-specific tables are also created
    
    # Clean relevant tables before each test to ensure isolation
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM operacoes")
        cursor.execute("DELETE FROM carteira_atual") # Corrected table name
        cursor.execute("DELETE FROM resultados_mensais")
        # cursor.execute("DELETE FROM darfs") # DARFs table does not seem to exist, info is in resultados_mensais
        # Clean auth tables as well, or ensure they are handled by test_auth.py's fixtures if run together
        cursor.execute("DELETE FROM tokens")
        cursor.execute("DELETE FROM usuario_funcoes")
        cursor.execute("DELETE FROM usuarios")
        cursor.execute("DELETE FROM funcoes WHERE nome NOT IN ('admin', 'usuario')") # Keep default roles
        conn.commit()

    # Create a standard user for most tests
    try:
        user_id = auth_criar_usuario(
            username=new_user_data["username"],
            email=new_user_data["email"],
            senha=new_user_data["senha"],
            nome_completo=new_user_data["nome_completo"]
        )
    except ValueError: # User might already exist if not cleaned properly, try to fetch
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM usuarios WHERE username = ?", (new_user_data["username"],))
            user_id_row = cursor.fetchone()
            if not user_id_row: # If still not found, fail the setup
                pytest.fail(f"Failed to create or find standard user {new_user_data['username']} for API tests.")
            user_id = user_id_row[0]
            
    # Create an admin user and assign admin role for admin tests
    try:
        admin_id = auth_criar_usuario(
            username=admin_login_data["username"],
            email="admin_api@example.com", # Use a different email to avoid collision if tests run in parallel or share db state
            senha=admin_login_data["password"],
            nome_completo="Admin API Test User"
        )
        adicionar_funcao_usuario(admin_id, "admin")
    except ValueError: # Admin might already exist
         with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM usuarios WHERE username = ?", (admin_login_data["username"],))
            admin_id_row = cursor.fetchone()
            if admin_id_row:
                admin_id = admin_id_row[0]
                adicionar_funcao_usuario(admin_id, "admin") # Ensure admin role
            else: # This case should ideally not happen if cleanup is correct
                pytest.fail(f"Failed to create or find admin user {admin_login_data['username']} for API tests.")


@pytest.fixture
async def auth_headers():
    # Ensure user for whom headers are being created exists.
    # setup_database_for_api_tests should handle this.
    user_to_login = new_user_data["username"]
    password_to_login = new_user_data["senha"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM usuarios WHERE username = ?", (user_to_login,))
        user_row = cursor.fetchone()
        if not user_row:
            pytest.fail(f"User {user_to_login} not found in DB for auth_headers fixture setup.")
        user_id = user_row[0]
        # Clear any existing tokens for this user to prevent collision
        cursor.execute("DELETE FROM tokens WHERE usuario_id = ?", (user_id,))
        conn.commit()

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        login_payload = {"username": user_to_login, "password": password_to_login}
        login_response = await ac.post("/auth/login", data=login_payload)
        
        assert login_response.status_code == status.HTTP_200_OK, \
            f"Failed to login standard user ({user_to_login}) for API tests. Response: {login_response.text}"
        
        token = login_response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

@pytest.fixture
async def admin_auth_headers():
    admin_username_to_login = admin_login_data["username"]
    admin_password_to_login = admin_login_data["password"]

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM usuarios WHERE username = ?", (admin_username_to_login,))
        user_row = cursor.fetchone()
        if not user_row:
            pytest.fail(f"Admin User {admin_username_to_login} not found in DB for admin_auth_headers fixture setup.")
        user_id = user_row[0]
        # Clear any existing tokens for this admin user
        cursor.execute("DELETE FROM tokens WHERE usuario_id = ?", (user_id,))
        conn.commit()
        
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        admin_login_payload = {"username": admin_username_to_login, "password": admin_password_to_login}
        login_response = await ac.post("/auth/login", data=admin_login_payload)

        assert login_response.status_code == status.HTTP_200_OK, \
            f"Failed to login admin user ({admin_username_to_login}) for API tests: {login_response.text}"

        token = login_response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

@pytest.mark.asyncio
async def test_create_operation(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post("/operacoes", json=sample_operation_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["mensagem"] == "Operação criada com sucesso."

@pytest.mark.asyncio
async def test_list_operations(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Create an operation first
        await ac.post("/operacoes", json=sample_operation_data, headers=headers)
        response = await ac.get("/operacoes", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    operations = response.json()
    assert isinstance(operations, list)
    assert len(operations) > 0
    assert operations[0]["ticker"] == sample_operation_data["ticker"]

@pytest.mark.asyncio
async def test_delete_operation(auth_headers):
    headers = await auth_headers
    operation_id_to_delete = -1 # Default to an invalid ID

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Create an operation
        create_response = await ac.post("/operacoes", json=sample_operation_data, headers=headers)
        assert create_response.status_code == status.HTTP_200_OK
        
        # List operations to get its ID
        list_response = await ac.get("/operacoes", headers=headers)
        assert list_response.status_code == status.HTTP_200_OK
        operations_list = list_response.json()
        assert len(operations_list) > 0, "No operations found to delete"
        operation_id_to_delete = operations_list[0]["id"]
        
        delete_response = await ac.delete(f"/operacoes/{operation_id_to_delete}", headers=headers)
    assert delete_response.status_code == status.HTTP_200_OK
    assert delete_response.json()["mensagem"] == f"Operação {operation_id_to_delete} removida com sucesso."

    # Verify it's deleted
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac_after_delete:
        list_after_delete_response = await ac_after_delete.get("/operacoes", headers=headers)
        assert list_after_delete_response.status_code == status.HTTP_200_OK
        assert all(op['id'] != operation_id_to_delete for op in list_after_delete_response.json())


@pytest.mark.asyncio
async def test_upload_operations(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        file_content_str = json.dumps(sample_operations_file_content) # Ensure it's a string
        files = {"file": ("operacoes.json", file_content_str.encode('utf-8'), "application/json")}
        response = await ac.post("/upload", files=files, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    response_data = response.json()
    assert "operações importadas" in response_data["mensagem"]
    # Check for the number of operations based on the sample data
    expected_num_ops = len(sample_operations_file_content)
    assert str(expected_num_ops) in response_data["mensagem"]


@pytest.mark.asyncio
async def test_get_resultados(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Add some operations that would generate results
        await ac.post("/operacoes", json={**sample_operation_data, "ticker": "TICKA", "operation": "buy", "price": 10, "quantity": 100}, headers=headers)
        await ac.post("/operacoes", json={**sample_operation_data, "ticker": "TICKA", "operation": "sell", "price": 12, "quantity": 50, "date": "2023-01-20"}, headers=headers)
        
        response = await ac.get("/resultados", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    assert isinstance(response.json(), list)
    # Further assertions can be made based on expected results from sample_operation_data

@pytest.mark.asyncio
async def test_get_carteira(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        await ac.post("/operacoes", json=sample_operation_data, headers=headers)
        response = await ac.get("/carteira", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    carteira = response.json()
    assert isinstance(carteira, list)
    # Further assertions based on sample_operation_data

@pytest.mark.asyncio
async def test_get_darfs(auth_headers):
    headers = await auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
         # Add operations that might generate DARFs (e.g., profitable sale)
        await ac.post("/operacoes", json={**sample_operation_data, "ticker": "TICKB", "operation": "buy", "price": 20, "quantity": 100, "date": "2023-03-01"}, headers=headers)
        await ac.post("/operacoes", json={**sample_operation_data, "ticker": "TICKB", "operation": "sell", "price": 25, "quantity": 100, "date": "2023-03-15"}, headers=headers)

        response = await ac.get("/darfs", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    assert isinstance(response.json(), list)
    # Further assertions based on DARF generation logic

@pytest.mark.asyncio
async def test_reset_database_admin_only(admin_auth_headers, auth_headers): # Needs admin and regular user
    headers = await auth_headers
    admin_headers = await admin_auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Try with non-admin user first (should fail)
        response_non_admin = await ac.delete("/reset", headers=headers)
        assert response_non_admin.status_code == status.HTTP_403_FORBIDDEN

        # Try with admin user (should succeed)
        response_admin = await ac.delete("/reset", headers=admin_headers)
    assert response_admin.status_code == status.HTTP_200_OK
    assert response_admin.json()["mensagem"] == "Banco de dados limpo com sucesso."

    # Verify data is cleared for the regular user
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac_after_reset:
        # Re-login the standard user.
        # The user defined by new_user_data should have been deleted by the reset.
        # The /reset endpoint currently only clears application data, not users.
        # So, the standard user ("testuser") should still exist. We log them back in.
        # Clear tokens for "testuser" before this specific login attempt to avoid collision after reset
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM usuarios WHERE username = ?", (new_user_data["username"],))
            user_row = cursor.fetchone()
            if user_row:
                cursor.execute("DELETE FROM tokens WHERE usuario_id = ?", (user_row[0],))
                conn.commit()
            # If user_row is None here, it means the user was unexpectedly deleted by /reset, which is an issue itself.
            # However, the auth_headers fixture already checks for user existence and fails if not found.
            # This is an additional safeguard for this specific test's re-login attempt.

        login_payload = {"username": new_user_data["username"], "password": new_user_data["senha"]}
        login_resp_after_reset = await ac_after_reset.post("/auth/login", data=login_payload)
        
        assert login_resp_after_reset.status_code == status.HTTP_200_OK, \
            f"Failed to re-login standard user after reset. User might have been deleted, or other issue. Response: {login_resp_after_reset.text}"
        
        # Use the new token to check operations
        new_auth_headers_after_reset = {"Authorization": f"Bearer {login_resp_after_reset.json()['access_token']}"}
        list_ops_response = await ac_after_reset.get("/operacoes", headers=new_auth_headers_after_reset)
        
        assert list_ops_response.status_code == status.HTTP_200_OK
        assert list_ops_response.json() == [], "Operations list for standard user is not empty after admin reset."


# More tests can be added for:
# - Edge cases (e.g., empty uploads, invalid data)
# - Specific calculations for resultados, carteira, darfs based on known inputs
# - Pagination if implemented
# - Error responses for invalid inputs or unauthorized access to these endpoints

# Ensure ASGITransport is used for AsyncClient if not already standard in the test setup
# This was added to the import line: from httpx import AsyncClient, ASGITransport
# All AsyncClient calls should use transport=ASGITransport(app=app) if they are not already.
# The provided code already uses app=app, which is fine for recent httpx versions with FastAPI.
# If issues arise, change to: AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL)
# Corrected this in fixtures auth_headers and admin_auth_headers.
# Corrected this in all test functions.
