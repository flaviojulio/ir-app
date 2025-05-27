"use client"

import type React from "react"

import { useState, useRef } from "react"
import { useRouter } from "next/navigation"
import { AuthGuard } from "@/components/auth-guard"
import { SidebarProvider } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, ArrowLeft, Upload, FileText, CheckCircle, AlertCircle } from "lucide-react"
import { operacoesAPI, handleAPIError } from "@/lib/api"
import { toast } from "@/hooks/use-toast"
import Link from "next/link"

export default function UploadOperacoesPage() {
  const router = useRouter()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [uploadResult, setUploadResult] = useState<string | null>(null)

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      if (file.type === "application/json" || file.name.endsWith(".json")) {
        setSelectedFile(file)
        setUploadResult(null)
      } else {
        toast({
          title: "Erro",
          description: "Apenas arquivos JSON são aceitos",
          variant: "destructive",
        })
      }
    }
  }

  const handleUpload = async () => {
    if (!selectedFile) return

    try {
      setIsLoading(true)
      const response = await operacoesAPI.uploadArquivo(selectedFile)

      setUploadResult(response.mensagem)
      toast({
        title: "Sucesso",
        description: response.mensagem,
      })

      // Clear form
      setSelectedFile(null)
      if (fileInputRef.current) {
        fileInputRef.current.value = ""
      }
    } catch (error) {
      handleAPIError(error)
    } finally {
      setIsLoading(false)
    }
  }

  const exampleData = [
    {
      date: "2024-01-15",
      ticker: "PETR4",
      operation: "buy",
      quantity: 100,
      price: 28.5,
      fees: 15.9,
    },
    {
      date: "2024-01-20",
      ticker: "PETR4",
      operation: "sell",
      quantity: 50,
      price: 32.15,
      fees: 12.5,
    },
  ]

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
                      <h1 className="text-2xl font-bold">Upload de Operações</h1>
                      <p className="text-muted-foreground">Importe múltiplas operações de um arquivo JSON</p>
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
                <div className="max-w-4xl mx-auto space-y-6">
                  {/* Upload Form */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Selecionar Arquivo</CardTitle>
                      <CardDescription>Escolha um arquivo JSON com suas operações</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center">
                        <FileText className="mx-auto h-12 w-12 text-gray-400" />
                        <div className="mt-4">
                          <input
                            ref={fileInputRef}
                            type="file"
                            accept=".json"
                            onChange={handleFileSelect}
                            className="hidden"
                            id="file-upload"
                          />
                          <label
                            htmlFor="file-upload"
                            className="cursor-pointer inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
                          >
                            <Upload className="h-4 w-4 mr-2" />
                            Escolher Arquivo
                          </label>
                          <p className="mt-2 text-sm text-gray-500">Apenas arquivos JSON são aceitos</p>
                        </div>
                      </div>

                      {selectedFile && (
                        <div className="flex items-center justify-between p-4 bg-blue-50 rounded-lg">
                          <div className="flex items-center">
                            <FileText className="h-5 w-5 text-blue-600 mr-2" />
                            <span className="text-sm font-medium">{selectedFile.name}</span>
                            <span className="text-sm text-gray-500 ml-2">
                              ({(selectedFile.size / 1024).toFixed(1)} KB)
                            </span>
                          </div>
                          <Button onClick={handleUpload} disabled={isLoading} size="sm">
                            {isLoading ? (
                              <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Processando...
                              </>
                            ) : (
                              <>
                                <Upload className="mr-2 h-4 w-4" />
                                Fazer Upload
                              </>
                            )}
                          </Button>
                        </div>
                      )}

                      {uploadResult && (
                        <div className="flex items-center p-4 bg-green-50 rounded-lg">
                          <CheckCircle className="h-5 w-5 text-green-600 mr-2" />
                          <span className="text-sm text-green-800">{uploadResult}</span>
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  {/* Format Instructions */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Formato do Arquivo</CardTitle>
                      <CardDescription>O arquivo JSON deve seguir o formato abaixo</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-4">
                        <div className="p-4 bg-gray-50 rounded-lg">
                          <h4 className="font-semibold mb-2">Campos obrigatórios:</h4>
                          <ul className="text-sm space-y-1">
                            <li>
                              <strong>date:</strong> Data da operação (formato: YYYY-MM-DD)
                            </li>
                            <li>
                              <strong>ticker:</strong> Código da ação (ex: "PETR4")
                            </li>
                            <li>
                              <strong>operation:</strong> Tipo de operação ("buy" ou "sell")
                            </li>
                            <li>
                              <strong>quantity:</strong> Quantidade de ações (número inteiro)
                            </li>
                            <li>
                              <strong>price:</strong> Preço por ação (número decimal)
                            </li>
                            <li>
                              <strong>fees:</strong> Taxas e corretagem (número decimal, opcional)
                            </li>
                          </ul>
                        </div>

                        <div>
                          <h4 className="font-semibold mb-2">Exemplo de arquivo JSON:</h4>
                          <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg text-sm overflow-x-auto">
                            {JSON.stringify(exampleData, null, 2)}
                          </pre>
                        </div>

                        <div className="p-4 bg-yellow-50 rounded-lg">
                          <div className="flex items-start">
                            <AlertCircle className="h-5 w-5 text-yellow-600 mr-2 mt-0.5" />
                            <div className="text-sm">
                              <p className="font-semibold text-yellow-800 mb-1">Importante:</p>
                              <ul className="text-yellow-700 space-y-1">
                                <li>• O arquivo deve conter um array de operações</li>
                                <li>• Todas as operações serão processadas e a carteira será recalculada</li>
                                <li>• Operações duplicadas podem causar inconsistências</li>
                                <li>• Recomendamos fazer backup antes de importar</li>
                              </ul>
                            </div>
                          </div>
                        </div>
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
