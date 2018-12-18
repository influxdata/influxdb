import {Authorization} from 'src/api'
import {authorizationsAPI} from 'src/utils/api'
import {AxiosResponse} from 'axios'

export const getAuthorizations = async (): Promise<Authorization[]> => {
  const {data} = await authorizationsAPI.authorizationsGet()
  return data.authorizations
}

export const deleteAuthorization = async (
  authID: string
): Promise<AxiosResponse> => {
  const response = await authorizationsAPI.authorizationsAuthIDDelete(authID)
  return response
}
