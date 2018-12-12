import {usersAPI} from 'src/utils/api'
import {User} from 'src/api'

export const getMe = async (): Promise<User> => {
  const {data} = await usersAPI.meGet()
  return data
}
