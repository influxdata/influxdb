export interface UserToken {
  id: string
  name: string
  secretKey: string
}

export interface User {
  id: string
  name: string
  email: string
  avatar: string
  tokens: UserToken[]
}
