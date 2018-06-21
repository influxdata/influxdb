import * as AuthData from 'src/types/auth'

export interface Auth {
  auth: {
    isUsingAuth: boolean
    me: AuthData.Me
  }
}
