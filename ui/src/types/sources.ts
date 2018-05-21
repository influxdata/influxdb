import {Kapacitor, Service} from './'

export interface Source {
  id: string
  name: string
  type: string
  username?: string
  password?: string
  sharedSecret?: string
  url: string
  metaUrl?: string
  insecureSkipVerify: boolean
  default: boolean
  telegraf: string
  organization: string
  role: string
  defaultRP: string
  links: SourceLinks
  kapacitors?: Kapacitor[] // this field does not exist on the server type for Source and is added in the client in the reducer for loading kapacitors.
  services?: Service[]
}

export interface SourceLinks {
  self: string
  kapacitors: string
  proxy: string
  queries: string
  write: string
  permissions: string
  users: string
  roles?: string
  databases: string
  annotations: string
  health: string
  services: string
}
