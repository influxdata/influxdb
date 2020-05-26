import AJAX from 'src/utils/ajax'
import {Authorization, Auth0Config} from 'src/types'
import {getAPIBasepath} from 'src/utils/basepath'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

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

export const getAuth0Config = async (
  redirectTo: string
): Promise<Auth0Config> => {
  try {
    let url = `${getAPIBasepath()}/api/v2private/oauth/clientConfig`
    if (isFlagEnabled('redirectto')) {
      url = `${getAPIBasepath()}/api/v2private/oauth/clientConfig?redirectTo=${redirectTo}`
    }
    const response = await fetch(url)
    const data = await response.json()
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
