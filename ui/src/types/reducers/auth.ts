import * as Types from 'src/types/modules'

export interface Auth {
  auth: {
    isUsingAuth: boolean
    me: Types.Auth.Data.Me
  }
}
