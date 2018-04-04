export interface Organization {
  defaultRole: string
  id: string
  links: {
    self: string
  }
  name: string
}

export interface Me {
  currentOrganization?: Organization
  role: Role
}

export interface Role {
  name: string
  organization: string
}

export interface User {
  id: string
  links: {self: string}
  name: string
  provider: string
  roles: Role[]
  scheme: string
  superAdmin: boolean
}

export interface Auth {
  callback: string
  label: string
  login: string
  logout: string
  name: string
}

export interface AuthConfig {
  auth: string
  self: string
}

export interface AuthLinks {
  allUsers: string
  auth: Auth[]
  config: AuthConfig
  dashboards: string
  environment: string
  external: {
    statusFeed?: string
  }
  layouts: string
  logout: string
  mappings: string
  me: string
  organizations: string
  sources: string
  users: string
}
