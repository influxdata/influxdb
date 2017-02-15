import axios from 'axios';

let links

const UNAUTHORIZED = 401

export default async function AJAX({
  url,
  resource,
  id,
  method = 'GET',
  data = {},
  params = {},
  headers = {},
}) {
  let response

  try {
    const basepath = window.basepath || ''

    url = `${basepath}${url}`

    if (!links) {
      const linksRes = response = await axios({
        url: `${basepath}/chronograf/v1`,
        method: 'GET',
      })
      links = linksRes.data
    }

    const {auth} = links

    if (resource) {
      url = id ? `${basepath}${links[resource]}/${id}` : `${basepath}${links[resource]}`
    }

    response = await axios({
      url,
      method,
      data,
      params,
      headers,
    })

    return {
      auth,
      ...response,
    }
  } catch (error) {
    if (!response.status === UNAUTHORIZED) {
      console.error(error) // eslint-disable-line no-console
    }
    throw {response} // eslint-disable-line no-throw-literal
  }
}
