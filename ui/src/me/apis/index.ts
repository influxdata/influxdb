import {baseAPI} from 'src/utils/api'

export const logout = async (): Promise<void> => {
  await baseAPI.signoutPost()
}
