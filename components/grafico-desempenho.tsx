"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import type { ResultadoMensal } from "@/lib/api"

interface GraficoDesempenhoProps {
  resultados: ResultadoMensal[]
}

export function GraficoDesempenho({ resultados }: GraficoDesempenhoProps) {
  // Transform data for chart
  const dadosGrafico = resultados.map((resultado) => ({
    mes: resultado.mes,
    ganho_swing: resultado.ganho_liquido_swing,
    ganho_day: resultado.ganho_liquido_day,
    total: resultado.ganho_liquido_swing + resultado.ganho_liquido_day,
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Desempenho Mensal</CardTitle>
        <CardDescription>Evolução dos seus resultados mensais</CardDescription>
      </CardHeader>
      <CardContent>
        {dadosGrafico.length === 0 ? (
          <div className="h-[300px] flex items-center justify-center">
            <p className="text-muted-foreground">Nenhum dado disponível</p>
          </div>
        ) : (
          <div className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={dadosGrafico}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="mes"
                  fontSize={12}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(value) => {
                    const [year, month] = value.split("-")
                    return `${month}/${year.slice(2)}`
                  }}
                />
                <YAxis
                  fontSize={12}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(value) => `R$ ${(value / 1000).toFixed(0)}k`}
                />
                <Tooltip
                  formatter={(value: number, name: string) => [
                    value.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                    }),
                    name === "ganho_swing" ? "Swing Trade" : name === "ganho_day" ? "Day Trade" : "Total",
                  ]}
                  labelFormatter={(label) => `Mês: ${label}`}
                />
                <Line
                  type="monotone"
                  dataKey="ganho_swing"
                  stroke="#22c55e"
                  strokeWidth={2}
                  dot={{ fill: "#22c55e", strokeWidth: 2, r: 4 }}
                  name="ganho_swing"
                />
                <Line
                  type="monotone"
                  dataKey="ganho_day"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={{ fill: "#3b82f6", strokeWidth: 2, r: 4 }}
                  name="ganho_day"
                />
                <Line
                  type="monotone"
                  dataKey="total"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  dot={{ fill: "#f59e0b", strokeWidth: 2, r: 4 }}
                  name="total"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
