import { toast } from "@/hooks/use-toast"

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

// Types for API responses
export interface User {
  id: number
  username: string
  email: string
  nome_completo: string
  funcoes: string[]
  data_criacao?: string
  data_atualizacao?: string
  ativo?: boolean
}

export interface LoginResponse {
  access_token: string
  token_type: string
}

export interface Operacao {
  id: number
  date: string
  ticker: string
  operation: "buy" | "sell"
  quantity: number
  price: number
  fees: number
  usuario_id?: number
}

export interface CarteiraItem {
  id?: number
  ticker: string
  quantidade: number
  custo_total: number
  preco_medio: number
}

export interface ResultadoMensal {
  id?: number
  mes: string
  vendas_swing: number
  custo_swing: number
  ganho_liquido_swing: number
  isento_swing: boolean
  ganho_liquido_day: number
  ir_devido_day: number
  irrf_day: number
  ir_pagar_day: number
  prejuizo_acumulado_swing: number
  prejuizo_acumulado_day: number
  darf_codigo?: string
  darf_competencia?: string
  darf_valor?: number
  darf_vencimento?: string
}

export interface DARF {
  codigo: string
  competencia: string
  valor: number
  vencimento: string
}

export interface OperacaoFechada {
  id?: number
  ticker: string
  data_abertura: string
  data_fechamento: string
  quantidade: number
  valor_compra: number
  valor_venda: number
  resultado: number
  percentual_lucro: number
  day_trade?: boolean
}

// Auth token management
class AuthManager {
  private static TOKEN_KEY = "auth_token"

  static getToken(): string | null {
    if (typeof window === "undefined") return null
    return localStorage.getItem(this.TOKEN_KEY)
  }

  static setToken(token: string): void {
    if (typeof window === "undefined") return
    localStorage.setItem(this.TOKEN_KEY, token)
  }

  static removeToken(): void {
    if (typeof window === "undefined") return
    localStorage.removeItem(this.TOKEN_KEY)
  }

  static isAuthenticated(): boolean {
    return !!this.getToken()
  }
}

// API Error handling
export class APIError extends Error {
  constructor(
    message: string,
    public status: number,
    public code?: string,
  ) {
    super(message)
    this.name = "APIError"
  }
}

// Base API client
class APIClient {
  private baseURL: string

  constructor(baseURL: string) {
    this.baseURL = baseURL
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseURL}${endpoint}`
    const token = AuthManager.getToken()

    const config: RequestInit = {
      headers: {
        "Content-Type": "application/json",
        ...(token && { Authorization: `Bearer ${token}` }),
        ...options.headers,
      },
      ...options,
    }

    try {
      const response = await fetch(url, config)

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}))

        // Handle authentication errors
        if (response.status === 401) {
          AuthManager.removeToken()
          if (typeof window !== "undefined") {
            window.location.href = "/login"
          }
        }

        throw new APIError(
          errorData.detail || errorData.message || "Erro na requisição",
          response.status,
          errorData.error_code,
        )
      }

      const contentType = response.headers.get("content-type")
      if (contentType && contentType.includes("application/json")) {
        return await response.json()
      }

      return {} as T
    } catch (error) {
      if (error instanceof APIError) {
        throw error
      }

      // Network or other errors
      throw new APIError("Erro de conexão. Verifique sua internet.", 0)
    }
  }

  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "GET" })
  }

  async post<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: "POST",
      body: data ? JSON.stringify(data) : undefined,
    })
  }

  async put<T>(endpoint: string, data?: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: "PUT",
      body: data ? JSON.stringify(data) : undefined,
    })
  }

  async delete<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "DELETE" })
  }

  async uploadFile<T>(endpoint: string, file: File): Promise<T> {
    const formData = new FormData()
    formData.append("file", file)

    return this.request<T>(endpoint, {
      method: "POST",
      body: formData,
      headers: {}, // Let browser set Content-Type for FormData
    })
  }
}

const apiClient = new APIClient(API_BASE_URL)

// Authentication API
export const authAPI = {
  async login(username: string, password: string): Promise<LoginResponse> {
    const formData = new FormData()
    formData.append("username", username)
    formData.append("password", password)

    const response = await fetch(`${API_BASE_URL}/api/auth/login`, {
      method: "POST",
      body: formData,
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({}))
      throw new APIError(error.detail || "Erro no login", response.status)
    }

    return response.json()
  },

  async register(userData: {
    username: string
    email: string
    senha: string
    nome_completo?: string
  }): Promise<User> {
    return apiClient.post<User>("/api/auth/registrar", userData)
  },

  async logout(): Promise<void> {
    try {
      await apiClient.post("/api/auth/logout")
    } finally {
      AuthManager.removeToken()
    }
  },

  async getMe(): Promise<User> {
    return apiClient.get<User>("/api/auth/me")
  },
}

// Operations API
export const operacoesAPI = {
  async listar(): Promise<Operacao[]> {
    return apiClient.get<Operacao[]>("/api/operacoes")
  },

  async criar(operacao: {
    date: string
    ticker: string
    operation: "buy" | "sell"
    quantity: number
    price: number
    fees?: number
  }): Promise<{ mensagem: string }> {
    return apiClient.post("/api/operacoes", operacao)
  },

  async deletar(operacaoId: number): Promise<{ mensagem: string }> {
    return apiClient.delete(`/api/operacoes/${operacaoId}`)
  },

  async uploadArquivo(file: File): Promise<{ mensagem: string }> {
    return apiClient.uploadFile("/api/upload", file)
  },

  async obterFechadas(): Promise<OperacaoFechada[]> {
    return apiClient.get<OperacaoFechada[]>("/api/operacoes/fechadas")
  },
}

// Portfolio API
export const carteiraAPI = {
  async obter(): Promise<CarteiraItem[]> {
    return apiClient.get<CarteiraItem[]>("/api/carteira")
  },
}

// Results API
export const resultadosAPI = {
  async obter(): Promise<ResultadoMensal[]> {
    return apiClient.get<ResultadoMensal[]>("/api/resultados")
  },
}

// DARF API
export const darfAPI = {
  async obter(): Promise<DARF[]> {
    return apiClient.get<DARF[]>("/api/darfs")
  },
}

// Export auth manager and API client
export { AuthManager, apiClient }

// Utility function for handling API errors with toast
export const handleAPIError = (error: unknown) => {
  if (error instanceof APIError) {
    toast({
      title: "Erro",
      description: error.message,
      variant: "destructive",
    })
  } else {
    toast({
      title: "Erro",
      description: "Ocorreu um erro inesperado",
      variant: "destructive",
    })
  }
}
