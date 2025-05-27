"use client"

import { useState, useEffect } from "react"
import {
  carteiraAPI,
  operacoesAPI,
  resultadosAPI,
  darfAPI,
  handleAPIError,
  type CarteiraItem,
  type Operacao,
  type ResultadoMensal,
  type DARF,
  type OperacaoFechada,
} from "@/lib/api"

// Custom hook for portfolio data
export function useCarteira() {
  const [carteira, setCarteira] = useState<CarteiraItem[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchCarteira = async () => {
    try {
      setIsLoading(true)
      setError(null)
      const data = await carteiraAPI.obter()
      setCarteira(data)
    } catch (error) {
      setError("Erro ao carregar carteira")
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchCarteira()
  }, [])

  return { carteira, isLoading, error, refetch: fetchCarteira }
}

// Custom hook for operations data
export function useOperacoes() {
  const [operacoes, setOperacoes] = useState<Operacao[]>([])
  const [operacoesFechadas, setOperacoesFechadas] = useState<OperacaoFechada[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchOperacoes = async () => {
    try {
      setIsLoading(true)
      setError(null)
      const [operacoesData, fechadasData] = await Promise.all([operacoesAPI.listar(), operacoesAPI.obterFechadas()])
      setOperacoes(operacoesData)
      setOperacoesFechadas(fechadasData)
    } catch (error) {
      setError("Erro ao carregar operações")
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  const deletarOperacao = async (operacaoId: number) => {
    try {
      await operacoesAPI.deletar(operacaoId)
      await fetchOperacoes() // Refresh data
    } catch (error) {
      handleAPIError(error)
      throw error
    }
  }

  useEffect(() => {
    fetchOperacoes()
  }, [])

  return {
    operacoes,
    operacoesFechadas,
    isLoading,
    error,
    refetch: fetchOperacoes,
    deletarOperacao,
  }
}

// Custom hook for results data
export function useResultados() {
  const [resultados, setResultados] = useState<ResultadoMensal[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchResultados = async () => {
    try {
      setIsLoading(true)
      setError(null)
      const data = await resultadosAPI.obter()
      setResultados(data)
    } catch (error) {
      setError("Erro ao carregar resultados")
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchResultados()
  }, [])

  return { resultados, isLoading, error, refetch: fetchResultados }
}

// Custom hook for DARF data
export function useDARF() {
  const [darfs, setDarfs] = useState<DARF[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchDarfs = async () => {
    try {
      setIsLoading(true)
      setError(null)
      const data = await darfAPI.obter()
      setDarfs(data)
    } catch (error) {
      setError("Erro ao carregar DARFs")
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchDarfs()
  }, [])

  return { darfs, isLoading, error, refetch: fetchDarfs }
}

// Combined hook for dashboard data
export function useDashboardData() {
  const { carteira, isLoading: carteiraLoading } = useCarteira()
  const { operacoes, isLoading: operacoesLoading } = useOperacoes()
  const { resultados, isLoading: resultadosLoading } = useResultados()
  const { darfs, isLoading: darfsLoading } = useDARF()

  const isLoading = carteiraLoading || operacoesLoading || resultadosLoading || darfsLoading

  return {
    carteira,
    operacoes,
    resultados,
    darfs,
    isLoading,
  }
}
