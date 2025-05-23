# Mock user data
new_user_data = {
    "username": "testuser",
    "email": "testuser@example.com",
    "senha": "testpassword",
    "nome_completo": "Test User"
}

login_data = {
    "username": "testuser",
    "password": "testpassword"
}

admin_user_data = {
    "username": "adminuser",
    "email": "adminuser@example.com",
    "senha": "adminpassword",
    "nome_completo": "Admin User"
}

admin_login_data = {
    "username": "adminuser",
    "password": "adminpassword"
}

# Mock operation data
sample_operation_data = {
    "date": "2023-01-15",
    "ticker": "PETR4",
    "operation": "buy",
    "quantity": 100,
    "price": 28.50,
    "fees": 5.20
}

sample_operations_file_content = [
  {
    "date": "2023-01-10",
    "ticker": "VALE3",
    "operation": "buy",
    "quantity": 50,
    "price": 80.00,
    "fees": 4.50
  },
  {
    "date": "2023-02-05",
    "ticker": "ITUB4",
    "operation": "buy",
    "quantity": 200,
    "price": 25.00,
    "fees": 6.00
  }
]
