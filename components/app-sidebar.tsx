"use client"

import {
  BarChart3,
  DollarSign,
  FileText,
  Home,
  PieChart,
  Settings,
  TrendingUp,
  Wallet,
  AlertTriangle,
  LogOut,
} from "lucide-react"

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import { Button } from "@/components/ui/button"
import { useAuth } from "@/hooks/use-auth"
import Link from "next/link"

const menuItems = [
  {
    title: "Dashboard",
    url: "/",
    icon: Home,
    description: "Visão geral da carteira",
  },
  {
    title: "Minha Carteira",
    url: "/carteira",
    icon: Wallet,
    description: "Ações em carteira",
  },
  {
    title: "Operações",
    url: "/operacoes",
    icon: TrendingUp,
    description: "Histórico de trades",
  },
  {
    title: "Resultados",
    url: "/resultados",
    icon: BarChart3,
    description: "Lucros e prejuízos",
  },
  {
    title: "DARF",
    url: "/darf",
    icon: FileText,
    description: "Impostos a pagar",
  },
  {
    title: "Prejuízo Acumulado",
    url: "/prejuizo",
    icon: AlertTriangle,
    description: "Compensação fiscal",
  },
  {
    title: "Relatórios",
    url: "/relatorios",
    icon: PieChart,
    description: "Gráficos e análises",
  },
]

export function AppSidebar() {
  const { user, logout } = useAuth()

  return (
    <Sidebar>
      <SidebarHeader className="border-b p-4">
        <div className="flex items-center gap-2">
          <DollarSign className="h-6 w-6 text-green-600" />
          <span className="font-bold text-lg">Minha Carteira</span>
        </div>
        {user && (
          <div className="mt-2">
            <p className="text-sm text-muted-foreground">{user.nome_completo || user.username}</p>
          </div>
        )}
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Menu Principal</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {menuItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild tooltip={item.description}>
                    <Link href={item.url}>
                      <item.icon className="h-4 w-4" />
                      <span>{item.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="border-t p-4">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild>
              <Link href="/configuracoes">
                <Settings className="h-4 w-4" />
                <span>Configurações</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <Button variant="ghost" className="w-full justify-start p-2" onClick={() => logout()}>
              <LogOut className="h-4 w-4 mr-2" />
              Sair
            </Button>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  )
}
