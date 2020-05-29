import axios, {AxiosResponse, Method} from 'axios'
import {getAPIBasepath} from 'src/utils/basepath'

// do not prefix route with basepath, ex. for external links
const addBasepath = (url, excludeBasepath): string => {
  return excludeBasepath ? url : `${getAPIBasepath()}${url}`
}

interface RequestParams {
  url?: string | string[]
  resource?: string | null
  id?: string | null
  method?: Method
  data?: object | string
  params?: object
  headers?: object
  auth?: {username: string; password: string}
}

/*
 * @deprecated
 *
 * Use fetch instead
 * @ see `runQuery` in src/shared/apis/query.ts for an example
 */
async function AJAX<T = any>(
  {
    url,
    method = 'GET',
    data = {},
    params = {},
    headers = {},
    auth = null,
  }: RequestParams,
  excludeBasepath = false
): Promise<T | AxiosResponse<T>> {
  try {
    url = addBasepath(url, excludeBasepath)

    const response = await axios.request<T>({
      url,
      method,
      data,
      params,
      headers,
      auth,
    })

    return response
  } catch (error) {
    const {response} = error
    throw response
  }
}

export async function getAJAX<T = any>(url: string): Promise<AxiosResponse<T>> {
  try {
    return await axios.request<T>({method: 'GET', url: addBasepath(url, false)})
  } catch (error) {
    console.error(error)
    throw error
  }
}

export default AJAX
