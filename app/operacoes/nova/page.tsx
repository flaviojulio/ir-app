"use client"

import type React from "react"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { AuthGuard } from "@/components/auth-guard"
import { SidebarProvider } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Loader2, ArrowLeft, Upload } from "lucide-react"
import { operacoesAPI, handleAPIError } from "@/lib/api"
import { toast } from "@/hooks/use-toast"
import Link from "next/link"

export default function NovaOperacaoPage() {
  const router = useRouter()
  const [isLoading, setIsLoading] = useState(false)
  const [formData, setFormData] = useState({
    date: new Date().toISOString().split("T")[0],
    ticker: "",
    operation: "" as "buy" | "sell" | "",
    quantity: "",
    price: "",
    fees: "",
  })

  const handleChange = (field: string, value: string) => {
    setFormData((prev) => ({
      ...prev,
      [field]: value,
    }))
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!formData.ticker || !formData.operation || !formData.quantity || !formData.price) {
      toast({
        title: "Erro",
        description: "Preencha todos os campos obrigatórios",
        variant: "destructive",
      })
      return
    }

    try {
      setIsLoading(true)

      await operacoesAPI.criar({
        date: formData.date,
        ticker: formData.ticker.toUpperCase(),
        operation: formData.operation,
        quantity: Number.parseInt(formData.quantity),
        price: Number.parseFloat(formData.price),
        fees: formData.fees ? Number.parseFloat(formData.fees) : 0,
      })

      toast({
        title: "Sucesso",
        description: "Operação criada com sucesso!",
      })

      router.push("/operacoes")
    } catch (error) {
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  const isFormValid =
    formData.ticker &&
    formData.operation &&
    formData.quantity &&
    formData.price &&
    !isNaN(Number(formData.quantity)) &&
    !isNaN(Number(formData.price)) &&
    Number(formData.quantity) > 0 &&
    Number(formData.price) > 0

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
                      <h1 className="text-2xl font-bold">Nova Operação</h1>
                      <p className="text-muted-foreground">Registre uma nova operação de compra ou venda</p>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button asChild variant="outline">
                      <Link href="/operacoes">
                        <ArrowLeft className="h-4 w-4 mr-2" />
                        Voltar
                      </Link>
                    </Button>
                  </div>
                </div>
              </header>

              {/* Main Content */}
              <div className="flex-1 p-6">
                <div className="max-w-2xl mx-auto space-y-6">
                  {/* Manual Form */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Dados da Operação</CardTitle>
                      <CardDescription>Preencha os dados da operação manualmente</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <form onSubmit={handleSubmit} className="space-y-4">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                          <div>
                            <Label htmlFor="date">Data *</Label>
                            <Input
                              id="date"
                              type="date"
                              value={formData.date}
                              onChange={(e) => handleChange("date", e.target.value)}
                              required
                            />
                          </div>

                          <div>
                            <Label htmlFor="ticker">Código da Ação *</Label>
                            <Input
                              id="ticker"
                              type="text"
                              value={formData.ticker}
                              onChange={(e) => handleChange("ticker", e.target.value.toUpperCase())}
                              placeholder="Ex: PETR4"
                              required
                            />
                          </div>

                          <div>
                            <Label htmlFor="operation">Tipo de Operação *</Label>
                            <Select
                              value={formData.operation}
                              onValueChange={(value) => handleChange("operation", value)}
                            >
                              <SelectTrigger>
                                <SelectValue placeholder="Selecione o tipo" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="buy">Compra</SelectItem>
                                <SelectItem value="sell">Venda</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>

                          <div>
                            <Label htmlFor="quantity">Quantidade *</Label>
                            <Input
                              id="quantity"
                              type="number"
                              value={formData.quantity}
                              onChange={(e) => handleChange("quantity", e.target.value)}
                              placeholder="100"
                              min="1"
                              required
                            />
                          </div>

                          <div>
                            <Label htmlFor="price">Preço por Ação *</Label>
                            <Input
                              id="price"
                              type="number"
                              step="0.01"
                              value={formData.price}
                              onChange={(e) => handleChange("price", e.target.value)}
                              placeholder="25.50"
                              min="0.01"
                              required
                            />
                          </div>

                          <div>
                            <Label htmlFor="fees">Taxas e Corretagem</Label>
                            <Input
                              id="fees"
                              type="number"
                              step="0.01"
                              value={formData.fees}
                              onChange={(e) => handleChange("fees", e.target.value)}
                              placeholder="15.90"
                              min="0"
                            />
                          </div>
                        </div>

                        {/* Summary */}
                        {formData.quantity && formData.price && (
                          <div className="p-4 bg-gray-50 rounded-lg">
                            <h4 className="font-semibold mb-2">Resumo da Operação</h4>
                            <div className="grid grid-cols-2 gap-4 text-sm">
                              <div>
                                <span className="text-muted-foreground">Valor Total:</span>
                                <span className="ml-2 font-medium">
                                  {(Number(formData.quantity) * Number(formData.price)).toLocaleString("pt-BR", {
                                    style: "currency",
                                    currency: "BRL",
                                  })}
                                </span>
                              </div>
                              <div>
                                <span className="text-muted-foreground">Taxas:</span>
                                <span className="ml-2 font-medium">
                                  {Number(formData.fees || 0).toLocaleString("pt-BR", {
                                    style: "currency",
                                    currency: "BRL",
                                  })}
                                </span>
                              </div>
                              <div className="col-span-2">
                                <span className="text-muted-foreground">Valor Final:</span>
                                <span className="ml-2 font-medium">
                                  {(
                                    Number(formData.quantity) * Number(formData.price) +
                                    Number(formData.fees || 0)
                                  ).toLocaleString("pt-BR", {
                                    style: "currency",
                                    currency: "BRL",
                                  })}
                                </span>
                              </div>
                            </div>
                          </div>
                        )}

                        <div className="flex gap-2">
                          <Button type="submit" disabled={isLoading || !isFormValid} className="flex-1">
                            {isLoading ? (
                              <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Salvando...
                              </>
                            ) : (
                              "Salvar Operação"
                            )}
                          </Button>
                          <Button type="button" variant="outline" asChild>
                            <Link href="/operacoes">Cancelar</Link>
                          </Button>
                        </div>
                      </form>
                    </CardContent>
                  </Card>

                  {/* File Upload Option */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Importar Arquivo</CardTitle>
                      <CardDescription>Ou importe múltiplas operações de um arquivo JSON</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="text-center">
                        <Button asChild variant="outline">
                          <Link href="/operacoes/upload">
                            <Upload className="h-4 w-4 mr-2" />
                            Fazer Upload de Arquivo
                          </Link>
                        </Button>
                        <p className="text-sm text-muted-foreground mt-2">Formatos aceitos: JSON</p>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </div>
            </div>
          </main>
        </div>
      </SidebarProvider>
    </AuthGuard>
  )
}
