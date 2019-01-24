import {client} from 'src/utils/api'

export const logout = async (): Promise<void> => {
  await client.auth.signout()
}
