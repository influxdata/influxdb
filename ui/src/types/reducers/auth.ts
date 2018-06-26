import * as AuthModels from 'src/types/auth'

export interface Auth {
  auth: {
    isUsingAuth: boolean
    me: AuthModels.Me
  }
}
