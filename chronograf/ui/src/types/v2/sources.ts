export type NewSource = Pick<Source, Exclude<keyof Source, 'id'>>

export enum SourceAuthenticationMethod {
  LDAP = 'ldap',
  Basic = 'basic',
  Shared = 'shared',
  Unknown = 'unknown',
}

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
  links: SourceLinks
  defaultRP?: string
  text?: string // added client-side for dropdowns
  authentication?: SourceAuthenticationMethod
}

export interface SourceLinks {
  self: string
  buckets: string
  query: string
}
