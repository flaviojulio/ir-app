import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Eye } from "lucide-react"
import Link from "next/link"
import type { CarteiraItem } from "@/lib/api"

interface CarteiraResumoProps {
  carteira: CarteiraItem[]
}

export function CarteiraResumo({ carteira }: CarteiraResumoProps) {
  // Show only first 4 items
  const carteiraLimitada = carteira.slice(0, 4)

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>Resumo da Carteira</CardTitle>
            <CardDescription>Suas principais ações e desempenho atual</CardDescription>
          </div>
          <Button asChild variant="outline">
            <Link href="/carteira">
              <Eye className="h-4 w-4 mr-2" />
              Ver Todas
            </Link>
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {carteiraLimitada.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-muted-foreground">Nenhuma ação na carteira</p>
              <Button asChild className="mt-4">
                <Link href="/operacoes/nova">Adicionar primeira operação</Link>
              </Button>
            </div>
          ) : (
            carteiraLimitada.map((item) => (
              <div
                key={item.ticker}
                className="flex items-center justify-between p-4 border rounded-lg hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center space-x-4">
                  <div>
                    <div className="flex items-center gap-2">
                      <h3 className="font-semibold">{item.ticker}</h3>
                      <Badge variant="secondary" className="text-xs">
                        Ações
                      </Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      {item.quantidade} ações • Preço médio:{" "}
                      {item.preco_medio.toLocaleString("pt-BR", {
                        style: "currency",
                        currency: "BRL",
                      })}
                    </p>
                  </div>
                </div>

                <div className="text-right">
                  <div className="font-semibold">
                    {item.custo_total.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    })}
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {item.preco_medio.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    })}
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </CardContent>
    </Card>
  )
}
