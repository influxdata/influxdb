export interface Source {
  id: string
  name: string
  url: string
  type: string
  default: boolean
  organization: string
  insecureSkipVerify: boolean
  role: string
  telegraf: string
  links: SourceLinks
  kapacitors?: Kapacitor[]
  metaUrl?: string
}

export interface SourceLinks {
  self: string
  kapacitors: string
  proxy: string
  queries: string
  write: string
  permissions: string
  users: string
  databases: string
  roles?: string
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
