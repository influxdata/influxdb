import axios, {AxiosResponse} from 'axios'

// do not prefix route with basepath, ex. for external links
const addBasepath = (url, excludeBasepath): string => {
  const basepath = window.basepath || ''

  return excludeBasepath ? url : `${basepath}${url}`
}

interface RequestParams {
  url?: string | string[]
  resource?: string | null
  id?: string | null
  method?: string
  data?: object | string
  params?: object
  headers?: object
}

async function AJAX<T = any>(
  {url, method = 'GET', data = {}, params = {}, headers = {}}: RequestParams,
  excludeBasepath = false
): Promise<(T) | AxiosResponse<T>> {
  try {
    url = addBasepath(url, excludeBasepath)

    const response = await axios.request<T>({
      url,
      method,
      data,
      params,
      headers,
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
