export interface Organization {
  defaultRole: string
  id: string
  links: {
    self: string
  }
  name: string
}

export interface Me {
  role: Role
  currentOrganization?: Organization
}

export interface AuthMe {
  id?: string
  role: string
  name: string
  provider: string
  scheme: string
  superAdmin: boolean
  logoutLink: string
  roles: Role[]
  links: {
    self: string
  }
  organizations?: Organization[]
  currentOrganization?: Organization
  isUsingAuth: boolean
  isMeLoading: boolean
  isAuthLoading: boolean
}

export enum InfluxDBPermissions {
  All = 'ALL',
  NoPermissions = 'NoPermissions',
  ViewAdmin = 'ViewAdmin',
  ViewChronograf = 'ViewChronograf',
  CreateDatabase = 'CreateDatabase',
  CreateUserAndRole = 'CreateUserAndRole',
  AddRemoveNode = 'AddRemoveNode',
  DropDatabase = 'DropDatabase',
  DropData = 'DropData',
  ReadData = 'ReadData',
  WriteData = 'WriteData',
  Rebalance = 'Rebalance',
  ManageShard = 'ManageShard',
  ManageContinuousQuery = 'ManageContinuousQuery',
  ManageQuery = 'ManageQuery',
  ManageSubscription = 'ManageSubscription',
  Monitor = 'Monitor',
  CopyShard = 'CopyShard',
  KapacitorAPI = 'KapacitorAPI',
  KapacitorConfigAPI = 'KapacitorConfigAPI',
}

export enum InfluxDBPermissionScope {
  All = 'all',
  Database = 'database',
}

export interface Permission {
  scope: string
  allowed: InfluxDBPermissions[]
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

export interface AuthLink {
  name: string
  label: string
  login: string
  logout: string
  callback: string
}
