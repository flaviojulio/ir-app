import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import status
import os
import sys

# Add the parent directory to the sys.path to allow imports from the main application
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app  # Assuming your FastAPI app instance is named 'app' in main.py
from test_data import new_user_data, login_data, admin_user_data, admin_login_data
from auth import criar_tabelas_autenticacao, get_db, adicionar_funcao_usuario

# Base URL for the API
BASE_URL = "http://localhost:8000/api" # Using localhost as tests run locally

@pytest.fixture(scope="function", autouse=True)
def setup_database():
    # Ensure auth tables are created before tests run
    criar_tabelas_autenticacao()
    
    # Clear relevant tables for a clean test environment
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tokens") # Clear all tokens
        cursor.execute("DELETE FROM usuario_funcoes") # Clear all role assignments
        cursor.execute("DELETE FROM usuarios") # Clear all users
        cursor.execute("DELETE FROM funcoes WHERE nome NOT IN ('admin', 'usuario')") # Clear test-specific roles
        conn.commit()

    # Re-insert default roles and create admin user for the session
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            from auth import criar_usuario # Local import
            admin_id = criar_usuario(
                username=admin_user_data["username"],
                email=admin_user_data["email"],
                senha=admin_user_data["senha"],
                nome_completo=admin_user_data["nome_completo"]
            )
            adicionar_funcao_usuario(admin_id, "admin")
            # Also add the 'usuario' role to admin, as some general functions might expect it or for broader testing
            adicionar_funcao_usuario(admin_id, "usuario")

    except Exception as e:
        print(f"Error setting up admin user: {e}")
        # If admin setup fails, many tests will break, this is critical.
        # Consider re-raising or pytest.fail if admin cannot be set up.


@pytest.mark.asyncio
async def test_register_user():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post("/auth/registrar", json=new_user_data)
    
    # This test specifically checks registration. It should succeed with a 200 if user doesn't exist.
    # If it was already created by setup_database (if new_user_data was admin for example), then this logic needs adjustment.
    # For now, new_user_data is distinct from admin_user_data.
    assert response.status_code == status.HTTP_200_OK, f"Registration failed: {response.text}"
    response_data = response.json()
    assert response_data["username"] == new_user_data["username"]
    assert response_data["email"] == new_user_data["email"]
    assert "id" in response_data

@pytest.mark.asyncio
async def test_login_user():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Ensure user is registered first.
        # A fixture to create this user once would be cleaner.
        reg_response = await ac.post("/auth/registrar", json=new_user_data)
        if not (reg_response.status_code == status.HTTP_200_OK or \
                (reg_response.status_code == status.HTTP_400_BAD_REQUEST and "já está em uso" in reg_response.text)):
            pytest.fail(f"Prerequisite: User registration for login test failed unexpectedly with {reg_response.status_code}: {reg_response.text}")

        response = await ac.post("/auth/login", data=login_data) 
    
    assert response.status_code == status.HTTP_200_OK, f"Login failed: {response.text}"
    response_data = response.json()
    assert "access_token" in response_data
    assert response_data["token_type"] == "bearer"

@pytest.mark.asyncio
async def test_get_me():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        reg_response = await ac.post("/auth/registrar", json=new_user_data)
        if not (reg_response.status_code == status.HTTP_200_OK or \
                (reg_response.status_code == status.HTTP_400_BAD_REQUEST and "já está em uso" in reg_response.text)):
            pytest.fail(f"Prerequisite: User registration for get_me test failed unexpectedly with {reg_response.status_code}: {reg_response.text}")

        login_response = await ac.post("/auth/login", data=login_data)
        assert login_response.status_code == status.HTTP_200_OK, f"Prerequisite: Login for get_me test failed: {login_response.text}"
        token = login_response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {token}"}
        response = await ac.get("/auth/me", headers=headers)
        
    assert response.status_code == status.HTTP_200_OK, f"Get me failed: {response.text}"
    response_data = response.json()
    assert response_data["username"] == new_user_data["username"]

@pytest.mark.asyncio
async def test_logout_user():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        reg_response = await ac.post("/auth/registrar", json=new_user_data)
        if not (reg_response.status_code == status.HTTP_200_OK or \
                (reg_response.status_code == status.HTTP_400_BAD_REQUEST and "já está em uso" in reg_response.text)):
            pytest.fail(f"Prerequisite: User registration for logout test failed unexpectedly with {reg_response.status_code}: {reg_response.text}")

        login_response = await ac.post("/auth/login", data=login_data)
        assert login_response.status_code == status.HTTP_200_OK, f"Prerequisite: Login for logout test failed: {login_response.text}"
        token = login_response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {token}"}
        response = await ac.post("/auth/logout", headers=headers)
        
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["mensagem"] == "Sessão encerrada com sucesso"

    # Try to use the token again, should fail
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac_after_logout:
        headers_after_logout = {"Authorization": f"Bearer {token}"}
        response_after_logout = await ac_after_logout.get("/auth/me", headers=headers_after_logout)
        assert response_after_logout.status_code == status.HTTP_401_UNAUTHORIZED


# --- Admin User Management Tests ---
@pytest.mark.asyncio
async def test_admin_list_users():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Login as admin
        admin_login_payload = {"username": admin_login_data["username"], "password": admin_login_data["password"]}
        login_response = await ac.post("/auth/login", data=admin_login_payload)
        assert login_response.status_code == status.HTTP_200_OK, f"Admin login failed: {login_response.text}"
        admin_token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {admin_token}"}

        response = await ac.get("/usuarios", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_admin_get_user_by_id():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        # Login as admin
        admin_login_payload = {"username": admin_login_data["username"], "password": admin_login_data["password"]}
        login_response = await ac.post("/auth/login", data=admin_login_payload)
        admin_token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {admin_token}"}

        # Get the admin's own ID by calling /auth/me
        me_response = await ac.get("/auth/me", headers=headers)
        assert me_response.status_code == status.HTTP_200_OK, "Failed to get admin's own data from /auth/me"
        admin_id = me_response.json()["id"]

        # Fetch the admin's user data using the obtained ID
        response = await ac.get(f"/usuarios/{admin_id}", headers=headers) 
    
    assert response.status_code == status.HTTP_200_OK, f"Failed to get user by ID: {response.text}"
    response_data = response.json()
    assert response_data["id"] == admin_id
    assert response_data["username"] == admin_user_data["username"]

# --- Admin Role Management Tests ---
@pytest.mark.asyncio
async def test_admin_list_funcoes():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        admin_login_payload = {"username": admin_login_data["username"], "password": admin_login_data["password"]}
        login_response = await ac.post("/auth/login", data=admin_login_payload)
        admin_token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {admin_token}"}

        response = await ac.get("/funcoes", headers=headers)
    assert response.status_code == status.HTTP_200_OK
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_admin_create_funcao():
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        admin_login_payload = {"username": admin_login_data["username"], "password": admin_login_data["password"]}
        login_response = await ac.post("/auth/login", data=admin_login_payload)
        admin_token = login_response.json()["access_token"]
        headers = {"Authorization": f"Bearer {admin_token}"}

        new_funcao_data = {"nome": "testrole", "descricao": "A test role"}
        response = await ac.post("/funcoes", json=new_funcao_data, headers=headers)
    assert response.status_code == status.HTTP_200_OK # Expect 200 based on main.py
    response_data = response.json()
    assert response_data["nome"] == new_funcao_data["nome"]

# Note: More tests for update, delete users/roles, assign/remove roles should be added
# For simplicity, focusing on a few key admin operations.
# Also, error handling (e.g., trying to access admin routes without admin rights) should be tested.
