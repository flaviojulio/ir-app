import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { History, Plus } from "lucide-react"
import Link from "next/link"
import type { Operacao } from "@/lib/api"

interface UltimasOperacoesProps {
  operacoes: Operacao[]
}

export function UltimasOperacoes({ operacoes }: UltimasOperacoesProps) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>Últimas Operações</CardTitle>
            <CardDescription>Suas operações mais recentes</CardDescription>
          </div>
          <Button asChild variant="outline" size="sm">
            <Link href="/operacoes">
              <History className="h-4 w-4 mr-2" />
              Ver Todas
            </Link>
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {operacoes.length === 0 ? (
            <div className="text-center py-4">
              <p className="text-muted-foreground text-sm">Nenhuma operação encontrada</p>
            </div>
          ) : (
            operacoes.map((operacao) => (
              <div key={operacao.id} className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-semibold">{operacao.ticker}</span>
                    <Badge
                      variant={operacao.operation === "buy" ? "default" : "secondary"}
                      className={
                        operacao.operation === "buy" ? "bg-blue-100 text-blue-800" : "bg-orange-100 text-orange-800"
                      }
                    >
                      {operacao.operation === "buy" ? "COMPRA" : "VENDA"}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    {operacao.quantity} ações •{" "}
                    {operacao.price.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    })}
                  </p>
                  <p className="text-xs text-muted-foreground">{new Date(operacao.date).toLocaleDateString("pt-BR")}</p>
                </div>

                <div className="text-right">
                  <div className="font-semibold">
                    {(operacao.quantity * operacao.price).toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    })}
                  </div>
                  {operacao.fees > 0 && (
                    <div className="text-xs text-muted-foreground">
                      Taxa:{" "}
                      {operacao.fees.toLocaleString("pt-BR", {
                        style: "currency",
                        currency: "BRL",
                      })}
                    </div>
                  )}
                </div>
              </div>
            ))
          )}
        </div>

        <div className="mt-4">
          <Button asChild className="w-full" variant="outline">
            <Link href="/operacoes/nova">
              <Plus className="h-4 w-4 mr-2" />
              Nova Operação
            </Link>
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
