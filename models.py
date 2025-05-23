from pydantic import BaseModel, Field, EmailStr, validator
from pydantic import ConfigDict
from typing import List, Optional, Literal
from datetime import date, datetime
from decimal import Decimal

# Modelos para autenticação

class UsuarioBase(BaseModel):
    username: str
    email: EmailStr
    nome_completo: Optional[str] = None

class UsuarioCreate(UsuarioBase):
    senha: str

class UsuarioUpdate(BaseModel):
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    nome_completo: Optional[str] = None
    senha: Optional[str] = None
    ativo: Optional[bool] = None

class UsuarioResponse(BaseModel):
    id: int
    username: str
    email: str
    nome_completo: str
    funcoes: List[str]
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    ativo: Optional[bool] = True    
    model_config = ConfigDict(from_attributes=True)

class LoginRequest(BaseModel):
    username_ou_email: str
    senha: str

class LoginResponse(BaseModel):
    usuario: UsuarioResponse
    token: str

class FuncaoBase(BaseModel):
    nome: str
    descricao: Optional[str] = None

class FuncaoCreate(FuncaoBase):
    pass

class FuncaoResponse(FuncaoBase):
    id: int
    
    model_config = ConfigDict(from_attributes=True)

class OperacaoBase(BaseModel):
    date: date
    ticker: str
    operation: str
    quantity: int
    price: float
    fees: Optional[float] = 0.0

class OperacaoCreate(OperacaoBase):
    pass

class Operacao(OperacaoBase):
    id: int
    usuario_id: Optional[int] = None

class ResultadoMensal(BaseModel):
    """
    Modelo para o resultado mensal de apuração de imposto de renda.
    """
    mes: str  # Formato: YYYY-MM
    vendas_swing: float
    custo_swing: float
    ganho_liquido_swing: float
    isento_swing: bool
    ganho_liquido_day: float
    ir_devido_day: float
    irrf_day: float
    ir_pagar_day: float
    prejuizo_acumulado_swing: float
    prejuizo_acumulado_day: float
    darf_codigo: Optional[str] = None
    darf_competencia: Optional[str] = None
    darf_valor: Optional[float] = None
    darf_vencimento: Optional[date] = None

    class Config:
        orm_mode = True

class CarteiraAtual(BaseModel):
    """
    Modelo para a carteira atual de ações.
    """
    ticker: str
    quantidade: int
    custo_total: float
    preco_medio: float

    class Config:
        orm_mode = True

class DARF(BaseModel):
    """
    Modelo para um DARF gerado.
    """
    codigo: str
    competencia: str
    valor: float
    vencimento: date

    class Config:
        orm_mode = True

# Modelo atualizado para a atualização da carteira
class AtualizacaoCarteira(BaseModel):
    """
    Modelo para atualização manual de um item da carteira.
    Permite alterar apenas a quantidade e o preço médio.
    """
    ticker: str
    quantidade: int
    preco_medio: float

    @validator('ticker')
    def ticker_uppercase(cls, v):
        """Converte o ticker para maiúsculo"""
        return v.upper()

    @validator('quantidade')
    def quantidade_positive(cls, v):
        """Valida se a quantidade é positiva ou zero"""
        if v < 0:
            raise ValueError('A quantidade deve ser um número positivo ou zero')
        return v

    @validator('preco_medio')
    def preco_medio_positive(cls, v, values):
        """Valida se o preço médio é positivo ou zero, e coerente com a quantidade"""
        if v < 0:
            raise ValueError('O preço médio deve ser um número positivo ou zero')
        
        # Se a quantidade for zero, o preço médio também deve ser zero
        if 'quantidade' in values and values['quantidade'] == 0 and v != 0:
            raise ValueError('Se a quantidade for zero, o preço médio também deve ser zero')
        
        return v

# Novos modelos para operações fechadas
class OperacaoDetalhe(BaseModel):
    """
    Modelo para detalhes de uma operação individual.
    """
    id: int
    date: date
    operation: str
    quantity: int
    price: float
    fees: float
    valor_total: float

    class Config:
        orm_mode = True

class OperacaoFechada(BaseModel):
    """
    Modelo para uma operação fechada (compra seguida de venda ou vice-versa).
    """
    ticker: str
    data_abertura: date
    data_fechamento: date
    tipo: str  # "compra-venda" ou "venda-compra" (venda a descoberto)
    quantidade: int
    preco_abertura: float
    preco_fechamento: float
    taxas_total: float
    resultado: float  # Lucro ou prejuízo
    operacoes_relacionadas: List[OperacaoDetalhe]
    day_trade: bool  # Indica se é day trade

    class Config:
        orm_mode = True

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

    class Config:
        orm_mode = True
    