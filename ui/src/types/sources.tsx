export interface Source {
  id: string
  name: string
  url: string
  links: {
    proxy: string
    self: string
    kapacitors: string
    queries: string
    permissions: string
    users: string
    databases: string
    roles: string
  }
  default: boolean
  telegraf?: string
  kapacitors?: Kapacitor[]
  metaUrl?: string
}

export interface Kapacitor {
  id?: string
  url: string
  name: string
  username?: string
  password?: string
  active: boolean
  links: {
    self: string
  }
}
