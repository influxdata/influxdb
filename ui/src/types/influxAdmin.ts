export interface UserRole {
  name: string
}

interface UserPermission {
  name: string
}

export interface User {
  name: string
  roles: UserRole[]
  permissions: UserPermission[]
  password: string
}
