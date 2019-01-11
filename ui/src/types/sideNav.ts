export interface Params {
  sourceID: string
}

export interface Location {
  pathname: string
}

export interface ExternalLink {
  name: string
  url: string
}

export interface ExternalLinks {
  custom: ExternalLink[]
}

export interface Links {
  me?: string
  external?: ExternalLinks
}

export interface Organization {
  id: string
  name: string
}

export interface Role {
  id?: string
  name?: string
}

export interface Me {
  name: string
  currentOrganization: Organization
  organizations: Organization[]
  role: Role[]
}
