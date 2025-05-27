import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { TrendingUp, TrendingDown, Search, Plus, Calendar, Download } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import Link from "next/link"

// Mock data das operações
const operacoes = [
  {
    id: 1,
    codigo: "PETR4",
    nome: "Petrobras PN",
    tipo: "COMPRA",
    quantidade: 50,
    preco: 31.2,
    data: "2024-01-15T10:30:00",
    corretagem: 15.9,
    impostos: 0,
    resultado: null,
    tipoOperacao: "Swing Trade",
  },
  {
    id: 2,
    codigo: "VALE3",
    nome: "Vale ON",
    tipo: "VENDA",
    quantidade: 25,
    preco: 62.5,
    data: "2024-01-14T14:45:00",
    corretagem: 12.5,
    impostos: 18.75,
    resultado: 125.5,
    tipoOperacao: "Day Trade",
  },
  {
    id: 3,
    codigo: "ITUB4",
    nome: "Itaú Unibanco PN",
    tipo: "COMPRA",
    quantidade: 100,
    preco: 26.8,
    data: "2024-01-12T09:15:00",
    corretagem: 20.0,
    impostos: 0,
    resultado: null,
    tipoOperacao: "Swing Trade",
  },
  {
    id: 4,
    codigo: "BBDC4",
    nome: "Bradesco PN",
    tipo: "VENDA",
    quantidade: 50,
    preco: 19.9,
    data: "2024-01-10T16:20:00",
    corretagem: 10.0,
    impostos: 0,
    resultado: -45.2,
    tipoOperacao: "Swing Trade",
  },
  {
    id: 5,
    codigo: "WEGE3",
    nome: "WEG ON",
    tipo: "COMPRA",
    quantidade: 75,
    preco: 42.8,
    data: "2024-01-08T11:00:00",
    corretagem: 18.5,
    impostos: 0,
    resultado: null,
    tipoOperacao: "Swing Trade",
  },
]

export default function OperacoesPage() {
  const totalOperacoes = operacoes.length
  const totalLucro = operacoes.reduce((total, op) => total + (op.resultado || 0), 0)
  const totalCorretagem = operacoes.reduce((total, op) => total + op.corretagem, 0)
  const totalImpostos = operacoes.reduce((total, op) => total + op.impostos, 0)

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <header className="border-b bg-white p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <SidebarTrigger />
            <div>
              <h1 className="text-2xl font-bold">Operações</h1>
              <p className="text-muted-foreground">Histórico completo de suas operações</p>
            </div>
          </div>
          <div className="flex gap-2">
            <Button variant="outline">
              <Download className="h-4 w-4 mr-2" />
              Exportar
            </Button>
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
        {/* Resumo das Operações */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Total de Operações</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totalOperacoes}</div>
              <p className="text-xs text-muted-foreground">Este mês</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Resultado Total</CardTitle>
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${totalLucro >= 0 ? "text-green-600" : "text-red-600"}`}>
                {totalLucro >= 0 ? "+" : ""}
                {totalLucro.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <div className={`flex items-center text-sm ${totalLucro >= 0 ? "text-green-600" : "text-red-600"}`}>
                {totalLucro >= 0 ? <TrendingUp className="h-4 w-4 mr-1" /> : <TrendingDown className="h-4 w-4 mr-1" />}
                Lucro/Prejuízo
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Corretagem Paga</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-orange-600">
                {totalCorretagem.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <p className="text-xs text-muted-foreground">Custos operacionais</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Impostos Pagos</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-red-600">
                {totalImpostos.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <p className="text-xs text-muted-foreground">Day Trade</p>
            </CardContent>
          </Card>
        </div>

        {/* Filtros e Lista de Operações */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Histórico de Operações</CardTitle>
                <CardDescription>Todas as suas operações de compra e venda</CardDescription>
              </div>
              <div className="flex gap-2">
                <div className="relative">
                  <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                  <Input placeholder="Buscar operação..." className="pl-8 w-64" />
                </div>
                <Select>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="Tipo" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="todos">Todos</SelectItem>
                    <SelectItem value="compra">Compra</SelectItem>
                    <SelectItem value="venda">Venda</SelectItem>
                  </SelectContent>
                </Select>
                <Select>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="Período" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="mes">Este mês</SelectItem>
                    <SelectItem value="trimestre">Trimestre</SelectItem>
                    <SelectItem value="ano">Este ano</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {operacoes.map((operacao) => {
                const valorTotal = operacao.quantidade * operacao.preco

                return (
                  <div key={operacao.id} className="border rounded-lg p-4 hover:bg-gray-50 transition-colors">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-4">
                        <div
                          className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                            operacao.tipo === "COMPRA" ? "bg-blue-100" : "bg-orange-100"
                          }`}
                        >
                          <span
                            className={`font-bold ${operacao.tipo === "COMPRA" ? "text-blue-600" : "text-orange-600"}`}
                          >
                            {operacao.tipo === "COMPRA" ? "C" : "V"}
                          </span>
                        </div>
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="font-semibold text-lg">{operacao.codigo}</h3>
                            <Badge
                              variant={operacao.tipo === "COMPRA" ? "default" : "secondary"}
                              className={
                                operacao.tipo === "COMPRA"
                                  ? "bg-blue-100 text-blue-800"
                                  : "bg-orange-100 text-orange-800"
                              }
                            >
                              {operacao.tipo}
                            </Badge>
                            <Badge variant="outline">{operacao.tipoOperacao}</Badge>
                          </div>
                          <p className="text-sm text-muted-foreground">{operacao.nome}</p>
                          <div className="flex items-center gap-2 text-xs text-muted-foreground">
                            <Calendar className="h-3 w-3" />
                            {new Date(operacao.data).toLocaleString("pt-BR")}
                          </div>
                        </div>
                      </div>

                      <div className="text-right">
                        <div className="font-semibold text-lg">
                          {valorTotal.toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </div>
                        <p className="text-sm text-muted-foreground">
                          {operacao.quantidade} ações × R$ {operacao.preco.toFixed(2)}
                        </p>
                        {operacao.resultado !== null && (
                          <div
                            className={`flex items-center justify-end text-sm ${operacao.resultado >= 0 ? "text-green-600" : "text-red-600"}`}
                          >
                            {operacao.resultado >= 0 ? (
                              <TrendingUp className="h-3 w-3 mr-1" />
                            ) : (
                              <TrendingDown className="h-3 w-3 mr-1" />
                            )}
                            {operacao.resultado >= 0 ? "+" : ""}
                            {operacao.resultado.toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
                      <div>
                        <p className="text-muted-foreground">Corretagem</p>
                        <p className="font-medium">
                          {operacao.corretagem.toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </p>
                      </div>
                      <div>
                        <p className="text-muted-foreground">Impostos</p>
                        <p className="font-medium">
                          {operacao.impostos.toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </p>
                      </div>
                      <div>
                        <p className="text-muted-foreground">Valor Líquido</p>
                        <p className="font-medium">
                          {(valorTotal - operacao.corretagem - operacao.impostos).toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </p>
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
