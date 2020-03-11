import AJAX from 'src/utils/ajax'
import {Authorization, Auth0Config} from 'src/types'

export const createAuthorization = async (
  authorization
): Promise<Authorization> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url: '/api/v2/authorizations',
      data: authorization,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getAuth0Config = async (): Promise<Auth0Config> => {
  try {
    const response = await fetch('/api/v2private/oauth/clientConfig')
    const data = await response.json()
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
