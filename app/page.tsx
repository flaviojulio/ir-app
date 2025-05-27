"use client"

import { useEffect, useState } from "react"
import { AuthGuard } from "@/components/auth-guard"
import { useAuth } from "@/hooks/use-auth"
import { SidebarProvider } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { TrendingDown, Wallet, BarChart3, AlertCircle, Plus, Loader2 } from "lucide-react"
import { CarteiraResumo } from "@/components/carteira-resumo"
import { UltimasOperacoes } from "@/components/ultimas-operacoes"
import { GraficoDesempenho } from "@/components/grafico-desempenho"
import { carteiraAPI, operacoesAPI, resultadosAPI, darfAPI, handleAPIError } from "@/lib/api"
import type { CarteiraItem, Operacao, ResultadoMensal, DARF } from "@/lib/api"
import Link from "next/link"

export default function Dashboard() {
  const { user } = useAuth()
  const [carteira, setCarteira] = useState<CarteiraItem[]>([])
  const [operacoes, setOperacoes] = useState<Operacao[]>([])
  const [resultados, setResultados] = useState<ResultadoMensal[]>([])
  const [darfs, setDarfs] = useState<DARF[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true)
        const [carteiraData, operacoesData, resultadosData, darfsData] = await Promise.all([
          carteiraAPI.obter(),
          operacoesAPI.listar(),
          resultadosAPI.obter(),
          darfAPI.obter(),
        ])

        setCarteira(carteiraData)
        setOperacoes(operacoesData)
        setResultados(resultadosData)
        setDarfs(darfsData)
      } catch (error) {
        handleAPIError(error)
      } finally {
        setIsLoading(false)
      }
    }

    fetchData()
  }, [])

  // Calculate dashboard metrics
  const valorTotalCarteira = carteira.reduce((total, item) => total + item.custo_total, 0)
  const totalAcoes = carteira.length
  const operacoesHoje = operacoes.filter((op) => {
    const hoje = new Date().toISOString().split("T")[0]
    return op.date.split("T")[0] === hoje
  }).length

  const impostosPendentes = darfs.reduce((total, darf) => total + darf.valor, 0)
  const ultimoResultado = resultados[resultados.length - 1]
  const prejuizoAcumulado = ultimoResultado?.prejuizo_acumulado_swing || 0

  if (isLoading) {
    return (
      <AuthGuard>
        <SidebarProvider>
          <div className="flex min-h-screen w-full">
            <AppSidebar />
            <main className="flex-1 overflow-auto">
              <div className="flex items-center justify-center min-h-screen">
                <Loader2 className="h-8 w-8 animate-spin" />
              </div>
            </main>
          </div>
        </SidebarProvider>
      </AuthGuard>
    )
  }

  return (
    <AuthGuard>
      <SidebarProvider>
        <div className="flex min-h-screen w-full">
          <AppSidebar />
          <main className="flex-1 overflow-auto">
            <div className="flex flex-col min-h-screen">
              {/* Header */}
              <header className="border-b bg-white p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <SidebarTrigger />
                    <div>
                      <h1 className="text-2xl font-bold">Dashboard</h1>
                      <p className="text-muted-foreground">Bem-vindo, {user?.nome_completo || user?.username}!</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button asChild>
                      <Link href="/operacoes/nova">
                        <Plus className="h-4 w-4 mr-2" />
                        Nova Operação
                      </Link>
                    </Button>
                  </div>
                </div>
              </header>

              {/* Main Content */}
              <div className="flex-1 p-6 space-y-6">
                {/* Cards de Resumo */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                  <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                      <CardTitle className="text-sm font-medium">Valor Total da Carteira</CardTitle>
                      <Wallet className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">
                        {valorTotalCarteira.toLocaleString("pt-BR", {
                          style: "currency",
                          currency: "BRL",
                        })}
                      </div>
                      <p className="text-xs text-muted-foreground">{totalAcoes} ações diferentes</p>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                      <CardTitle className="text-sm font-medium">Ações em Carteira</CardTitle>
                      <BarChart3 className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold">{totalAcoes}</div>
                      <p className="text-xs text-muted-foreground">{operacoesHoje} operações hoje</p>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                      <CardTitle className="text-sm font-medium">Impostos Pendentes</CardTitle>
                      <AlertCircle className="h-4 w-4 text-orange-500" />
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-orange-600">
                        {impostosPendentes.toLocaleString("pt-BR", {
                          style: "currency",
                          currency: "BRL",
                        })}
                      </div>
                      <p className="text-xs text-muted-foreground">{darfs.length} DARF(s) pendente(s)</p>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                      <CardTitle className="text-sm font-medium">Prejuízo Acumulado</CardTitle>
                      <TrendingDown className="h-4 w-4 text-red-500" />
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold text-red-600">
                        {prejuizoAcumulado.toLocaleString("pt-BR", {
                          style: "currency",
                          currency: "BRL",
                        })}
                      </div>
                      <p className="text-xs text-muted-foreground">Disponível para compensação</p>
                    </CardContent>
                  </Card>
                </div>

                {/* Seção Principal */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  {/* Resumo da Carteira */}
                  <div className="lg:col-span-2">
                    <CarteiraResumo carteira={carteira} />
                  </div>

                  {/* Últimas Operações */}
                  <div>
                    <UltimasOperacoes operacoes={operacoes.slice(-4)} />
                  </div>
                </div>

                {/* Gráfico de Desempenho */}
                <div>
                  <GraficoDesempenho resultados={resultados} />
                </div>

                {/* Alertas e Dicas */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertCircle className="h-5 w-5 text-blue-500" />
                      Dicas para Iniciantes
                    </CardTitle>
                    <CardDescription>
                      Informações importantes para quem está começando no mercado de ações
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="p-4 bg-blue-50 rounded-lg">
                        <h4 className="font-semibold text-blue-900 mb-2">💡 Diversificação</h4>
                        <p className="text-sm text-blue-800">
                          Não coloque todos os ovos na mesma cesta. Diversifique sua carteira entre diferentes setores e
                          empresas.
                        </p>
                      </div>
                      <div className="p-4 bg-green-50 rounded-lg">
                        <h4 className="font-semibold text-green-900 mb-2">📊 Acompanhe os Resultados</h4>
                        <p className="text-sm text-green-800">
                          Monitore regularmente o desempenho de suas ações e mantenha registros de suas operações.
                        </p>
                      </div>
                      <div className="p-4 bg-orange-50 rounded-lg">
                        <h4 className="font-semibold text-orange-900 mb-2">💰 Impostos</h4>
                        <p className="text-sm text-orange-800">
                          Lembre-se de pagar os impostos sobre seus lucros. Use nossa ferramenta de DARF para facilitar.
                        </p>
                      </div>
                      <div className="p-4 bg-purple-50 rounded-lg">
                        <h4 className="font-semibold text-purple-900 mb-2">📚 Estude Sempre</h4>
                        <p className="text-sm text-purple-800">
                          Continue aprendendo sobre o mercado financeiro. Conhecimento é seu melhor investimento.
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          </main>
        </div>
      </SidebarProvider>
    </AuthGuard>
  )
}
