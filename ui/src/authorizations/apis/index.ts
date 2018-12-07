import {Authorization} from 'src/api'
import {authorizationsAPI} from 'src/utils/api'

export const getAuthorizations = async (): Promise<Authorization[]> => {
  const {data} = await authorizationsAPI.authorizationsGet()
  return data.auths
}
