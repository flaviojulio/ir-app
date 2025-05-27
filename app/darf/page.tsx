import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { FileText, Download, Calculator, AlertTriangle, CheckCircle, Calendar } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

// Mock data para DARF
const darfData = [
  {
    id: 1,
    mesReferencia: "2024-01",
    vencimento: "2024-02-15",
    dayTrade: {
      lucro: 1250.0,
      imposto: 250.0, // 20%
      pago: true,
    },
    swingTrade: {
      lucro: 2500.0,
      imposto: 375.0, // 15%
      pago: false,
    },
    total: 625.0,
    status: "pendente",
  },
  {
    id: 2,
    mesReferencia: "2023-12",
    vencimento: "2024-01-15",
    dayTrade: {
      lucro: 800.0,
      imposto: 160.0,
      pago: true,
    },
    swingTrade: {
      lucro: 1200.0,
      imposto: 180.0,
      pago: true,
    },
    total: 340.0,
    status: "pago",
  },
  {
    id: 3,
    mesReferencia: "2023-11",
    vencimento: "2023-12-15",
    dayTrade: {
      lucro: 0,
      imposto: 0,
      pago: true,
    },
    swingTrade: {
      lucro: 3200.0,
      imposto: 480.0,
      pago: true,
    },
    total: 480.0,
    status: "pago",
  },
]

export default function DarfPage() {
  const totalPendente = darfData.filter((d) => d.status === "pendente").reduce((total, d) => total + d.total, 0)

  const totalPago = darfData.filter((d) => d.status === "pago").reduce((total, d) => total + d.total, 0)

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <header className="border-b bg-white p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <SidebarTrigger />
            <div>
              <h1 className="text-2xl font-bold">DARF - Impostos</h1>
              <p className="text-muted-foreground">Geração e controle de impostos sobre operações</p>
            </div>
          </div>
          <div className="flex gap-2">
            <Button variant="outline">
              <Calculator className="h-4 w-4 mr-2" />
              Calcular Impostos
            </Button>
            <Button>
              <FileText className="h-4 w-4 mr-2" />
              Gerar DARF
            </Button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 p-6 space-y-6">
        {/* Resumo dos Impostos */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Impostos Pendentes</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-orange-600">
                {totalPendente.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <div className="flex items-center text-sm text-orange-600">
                <AlertTriangle className="h-4 w-4 mr-1" />
                Vencimento próximo
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Impostos Pagos</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">
                {totalPago.toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <div className="flex items-center text-sm text-green-600">
                <CheckCircle className="h-4 w-4 mr-1" />
                Este ano
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Total Geral</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {(totalPendente + totalPago).toLocaleString("pt-BR", {
                  style: "currency",
                  currency: "BRL",
                })}
              </div>
              <p className="text-xs text-muted-foreground">Impostos totais</p>
            </CardContent>
          </Card>
        </div>

        {/* Explicação sobre Impostos */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator className="h-5 w-5 text-blue-500" />
              Como Funcionam os Impostos
            </CardTitle>
            <CardDescription>Entenda as regras de tributação para suas operações</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="p-4 bg-blue-50 rounded-lg">
                <h4 className="font-semibold text-blue-900 mb-2">📈 Day Trade</h4>
                <p className="text-sm text-blue-800 mb-2">Operações de compra e venda no mesmo dia</p>
                <ul className="text-xs text-blue-700 space-y-1">
                  <li>• Alíquota: 20% sobre o lucro</li>
                  <li>• Recolhimento até o último dia útil do mês seguinte</li>
                  <li>• Não há isenção</li>
                </ul>
              </div>
              <div className="p-4 bg-green-50 rounded-lg">
                <h4 className="font-semibold text-green-900 mb-2">📊 Swing Trade</h4>
                <p className="text-sm text-green-800 mb-2">Operações com prazo superior a um dia</p>
                <ul className="text-xs text-green-700 space-y-1">
                  <li>• Alíquota: 15% sobre o lucro</li>
                  <li>• Isenção para vendas até R$ 20.000/mês</li>
                  <li>• Recolhimento até o último dia útil do mês seguinte</li>
                </ul>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Lista de DARFs */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Histórico de DARFs</CardTitle>
                <CardDescription>Controle de impostos por período</CardDescription>
              </div>
              <div className="flex gap-2">
                <Select>
                  <SelectTrigger className="w-40">
                    <SelectValue placeholder="Ano" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="2024">2024</SelectItem>
                    <SelectItem value="2023">2023</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {darfData.map((darf) => (
                <div key={darf.id} className="border rounded-lg p-4 hover:bg-gray-50 transition-colors">
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div
                        className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                          darf.status === "pago" ? "bg-green-100" : "bg-orange-100"
                        }`}
                      >
                        {darf.status === "pago" ? (
                          <CheckCircle className="h-6 w-6 text-green-600" />
                        ) : (
                          <AlertTriangle className="h-6 w-6 text-orange-600" />
                        )}
                      </div>
                      <div>
                        <h3 className="font-semibold">
                          {new Date(darf.mesReferencia + "-01").toLocaleDateString("pt-BR", {
                            month: "long",
                            year: "numeric",
                          })}
                        </h3>
                        <div className="flex items-center gap-2 text-sm text-muted-foreground">
                          <Calendar className="h-3 w-3" />
                          Vencimento: {new Date(darf.vencimento).toLocaleDateString("pt-BR")}
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      <Badge variant={darf.status === "pago" ? "default" : "destructive"}>
                        {darf.status === "pago" ? "Pago" : "Pendente"}
                      </Badge>
                      <div className="text-right">
                        <div className="font-semibold text-lg">
                          {darf.total.toLocaleString("pt-BR", {
                            style: "currency",
                            currency: "BRL",
                          })}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="p-3 bg-blue-50 rounded-lg">
                      <h4 className="font-medium text-blue-900 mb-2">Day Trade</h4>
                      <div className="space-y-1 text-sm">
                        <div className="flex justify-between">
                          <span className="text-blue-700">Lucro:</span>
                          <span className="font-medium">
                            {darf.dayTrade.lucro.toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-blue-700">Imposto (20%):</span>
                          <span className="font-medium">
                            {darf.dayTrade.imposto.toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="p-3 bg-green-50 rounded-lg">
                      <h4 className="font-medium text-green-900 mb-2">Swing Trade</h4>
                      <div className="space-y-1 text-sm">
                        <div className="flex justify-between">
                          <span className="text-green-700">Lucro:</span>
                          <span className="font-medium">
                            {darf.swingTrade.lucro.toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-green-700">Imposto (15%):</span>
                          <span className="font-medium">
                            {darf.swingTrade.imposto.toLocaleString("pt-BR", {
                              style: "currency",
                              currency: "BRL",
                            })}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 flex justify-end gap-2">
                    <Button variant="outline" size="sm">
                      <FileText className="h-4 w-4 mr-2" />
                      Ver Detalhes
                    </Button>
                    {darf.status === "pendente" && (
                      <Button size="sm">
                        <Download className="h-4 w-4 mr-2" />
                        Gerar DARF
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
