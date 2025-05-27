import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { TrendingUp, TrendingDown, Search, Filter, Plus, Eye, MoreHorizontal } from "lucide-react"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import Link from "next/link"

// Mock data expandido das ações
const acoes = [
  {
    codigo: "PETR4",
    nome: "Petrobras PN",
    empresa: "Petróleo Brasileiro S.A.",
    quantidade: 100,
    precoMedio: 28.5,
    precoAtual: 32.15,
    setor: "Petróleo e Gás",
    variacao: 12.81,
    variacaoDia: 2.34,
    volume: 45230000,
    dividendos: 2.45,
  },
  {
    codigo: "VALE3",
    nome: "Vale ON",
    empresa: "Vale S.A.",
    quantidade: 50,
    precoMedio: 65.2,
    precoAtual: 61.8,
    setor: "Mineração",
    variacao: -5.21,
    variacaoDia: -1.23,
    volume: 23450000,
    dividendos: 8.9,
  },
  {
    codigo: "ITUB4",
    nome: "Itaú Unibanco PN",
    empresa: "Itaú Unibanco Holding S.A.",
    quantidade: 200,
    precoMedio: 25.3,
    precoAtual: 27.45,
    setor: "Bancos",
    variacao: 8.5,
    variacaoDia: 0.87,
    volume: 34560000,
    dividendos: 1.25,
  },
  {
    codigo: "BBDC4",
    nome: "Bradesco PN",
    empresa: "Banco Bradesco S.A.",
    quantidade: 150,
    precoMedio: 18.9,
    precoAtual: 19.75,
    setor: "Bancos",
    variacao: 4.5,
    variacaoDia: 1.12,
    volume: 28900000,
    dividendos: 0.95,
  },
  {
    codigo: "WEGE3",
    nome: "WEG ON",
    empresa: "WEG S.A.",
    quantidade: 75,
    precoMedio: 42.8,
    precoAtual: 45.2,
    setor: "Máquinas e Equipamentos",
    variacao: 5.61,
    variacaoDia: 0.45,
    volume: 12340000,
    dividendos: 1.8,
  },
]

export default function CarteiraPage() {
  const valorTotalCarteira = acoes.reduce((total, acao) => total + acao.quantidade * acao.precoAtual, 0)
  const lucroTotalCarteira = acoes.reduce(
    (total, acao) => total + (acao.precoAtual - acao.precoMedio) * acao.quantidade,
    0,
  )

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <header className="border-b bg-white p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <SidebarTrigger />
            <div>
              <h1 className="text-2xl font-bold">Minha Carteira</h1>
              <p className="text-muted-foreground">Gerencie suas ações e acompanhe o desempenho</p>
            </div>
          </div>
          <div className="flex gap-2">
            <Button variant="outline">
              <Filter className="h-4 w-4 mr-2" />
              Filtros
            </Button>
            <Button asChild>
              <Link href="/operacoes/nova">
                <Plus className="h-4 w-4 mr-2" />
                Adicionar Ação
              </Link>
            </Button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 p-6 space-y-6">
        {/* Resumo da Carteira */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Valor Total</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {valorTotalCarteira.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <p className="text-xs text-muted-foreground">{acoes.length} ações diferentes</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Lucro/Prejuízo Total</CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${lucroTotalCarteira >= 0 ? "text-green-600" : "text-red-600"}`}>
                {lucroTotalCarteira >= 0 ? "+" : ""}
                {lucroTotalCarteira.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <div
                className={`flex items-center text-sm ${lucroTotalCarteira >= 0 ? "text-green-600" : "text-red-600"}`}
              >
                {lucroTotalCarteira >= 0 ? (
                  <TrendingUp className="h-4 w-4 mr-1" />
                ) : (
                  <TrendingDown className="h-4 w-4 mr-1" />
                )}
                {((lucroTotalCarteira / (valorTotalCarteira - lucroTotalCarteira)) * 100).toFixed(2)}%
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Dividendos Recebidos</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-600">
                {acoes
                  .reduce((total, acao) => total + acao.dividendos * acao.quantidade, 0)
                  .toLocaleString("pt-BR", {
                    style: "currency",
                    currency: "BRL",
                  })}
              </div>
              <p className="text-xs text-muted-foreground">Últimos 12 meses</p>
            </CardContent>
          </Card>
        </div>

        {/* Filtros e Busca */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Ações em Carteira</CardTitle>
                <CardDescription>Detalhes de todas as suas posições</CardDescription>
              </div>
              <div className="flex gap-2">
                <div className="relative">
                  <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                  <Input placeholder="Buscar ação..." className="pl-8 w-64" />
                </div>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {acoes.map((acao) => {
                const valorTotal = acao.quantidade * acao.precoAtual
                const lucroTotal = (acao.precoAtual - acao.precoMedio) * acao.quantidade
                const percentualLucro = ((acao.precoAtual - acao.precoMedio) / acao.precoMedio) * 100

                return (
                  <div key={acao.codigo} className="border rounded-lg p-4 hover:bg-gray-50 transition-colors">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-4">
                        <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                          <span className="font-bold text-blue-600">{acao.codigo.slice(0, 2)}</span>
                        </div>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="font-semibold text-lg">{acao.codigo}</h3>
                            <Badge variant="secondary">{acao.setor}</Badge>
                          </div>
                          <p className="text-sm text-muted-foreground">{acao.nome}</p>
                          <p className="text-xs text-muted-foreground">{acao.empresa}</p>
                        </div>
                      </div>

                      <div className="text-right">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button variant="ghost" size="sm">
                              <MoreHorizontal className="h-4 w-4" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuItem>
                              <Eye className="h-4 w-4 mr-2" />
                              Ver Detalhes
                            </DropdownMenuItem>
                            <DropdownMenuItem>Comprar Mais</DropdownMenuItem>
                            <DropdownMenuItem>Vender</DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </div>
                    </div>

                    <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div>
                        <p className="text-xs text-muted-foreground">Quantidade</p>
                        <p className="font-semibold">{acao.quantidade} ações</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Preço Médio</p>
                        <p className="font-semibold">R$ {acao.precoMedio.toFixed(2)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Preço Atual</p>
                        <div className="flex items-center gap-1">
                          <p className="font-semibold">R$ {acao.precoAtual.toFixed(2)}</p>
                          <span className={`text-xs ${acao.variacaoDia >= 0 ? "text-green-600" : "text-red-600"}`}>
                            ({acao.variacaoDia >= 0 ? "+" : ""}
                            {acao.variacaoDia.toFixed(2)}%)
                          </span>
                        </div>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Valor Total</p>
                        <p className="font-semibold">
                          {valorTotal.toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </p>
                      </div>
                    </div>

                    <div className="mt-4 flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground">Lucro/Prejuízo</p>
                          <div className={`flex items-center ${lucroTotal >= 0 ? "text-green-600" : "text-red-600"}`}>
                            {lucroTotal >= 0 ? (
                              <TrendingUp className="h-4 w-4 mr-1" />
                            ) : (
                              <TrendingDown className="h-4 w-4 mr-1" />
                            )}
                            <span className="font-semibold">
                              {lucroTotal >= 0 ? "+" : ""}
                              {lucroTotal.toLocaleString("pt-BR", {
                                style: "currency",
                                currency: "BRL",
                              })}
                            </span>
                            <span className="ml-1 text-sm">
                              ({percentualLucro >= 0 ? "+" : ""}
                              {percentualLucro.toFixed(2)}%)
                            </span>
                          </div>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Volume (24h)</p>
                          <p className="text-sm">{(acao.volume / 1000000).toFixed(1)}M</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Dividendos</p>
                          <p className="text-sm text-blue-600">
                            {(acao.dividendos * acao.quantidade).toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
