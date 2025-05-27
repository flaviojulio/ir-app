"use client"

import { createContext, useContext, useEffect, useState, type ReactNode } from "react"
import { authAPI, AuthManager, type User, handleAPIError } from "@/lib/api"
import { useRouter } from "next/navigation"
import { toast } from "@/hooks/use-toast"

interface AuthContextType {
  user: User | null
  isLoading: boolean
  isAuthenticated: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => Promise<void>
  register: (userData: {
    username: string
    email: string
    senha: string
    nome_completo?: string
  }) => Promise<void>
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const router = useRouter()

  const isAuthenticated = !!user

  useEffect(() => {
    const initAuth = async () => {
      if (AuthManager.isAuthenticated()) {
        try {
          const userData = await authAPI.getMe()
          setUser(userData)
        } catch (error) {
          AuthManager.removeToken()
          handleAPIError(error)
        }
      }
      setIsLoading(false)
    }

    initAuth()
  }, [])

  const login = async (username: string, password: string) => {
    try {
      setIsLoading(true)
      const response = await authAPI.login(username, password)
      AuthManager.setToken(response.access_token)

      const userData = await authAPI.getMe()
      setUser(userData)

      toast({
        title: "Login realizado",
        description: `Bem-vindo, ${userData.nome_completo || userData.username}!`,
      })

      router.push("/")
    } catch (error) {
      handleAPIError(error)
      throw error
    } finally {
      setIsLoading(false)
    }
  }

  const logout = async () => {
    try {
      await authAPI.logout()
    } catch (error) {
      // Continue with logout even if API call fails
      console.error("Logout error:", error)
    } finally {
      AuthManager.removeToken()
      setUser(null)
      router.push("/login")

      toast({
        title: "Logout realizado",
        description: "Você foi desconectado com sucesso.",
      })
    }
  }

  const register = async (userData: {
    username: string
    email: string
    senha: string
    nome_completo?: string
  }) => {
    try {
      setIsLoading(true)
      await authAPI.register(userData)

      toast({
        title: "Conta criada",
        description: "Sua conta foi criada com sucesso. Faça login para continuar.",
      })

      router.push("/login")
    } catch (error) {
      handleAPIError(error)
      throw error
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        isLoading,
        isAuthenticated,
        login,
        logout,
        register,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return context
}
