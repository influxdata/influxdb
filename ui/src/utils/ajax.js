import axios from 'axios'

let links

export default async function AJAX({
  url,
  resource,
  id,
  method = 'GET',
  data = {},
  params = {},
  headers = {},
}) {
  try {
    const basepath = window.basepath || ''
    let response

    url = `${basepath}${url}`

    if (!links) {
      const linksRes = (response = await axios({
        url: `${basepath}/chronograf/v1`,
        method: 'GET',
      }))
      links = linksRes.data
    }

    const {auth} = links

    if (resource) {
      url = id
        ? `${basepath}${links[resource]}/${id}`
        : `${basepath}${links[resource]}`
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
    const {response} = error

    const {auth} = links
    throw {...response, auth: {links: auth}} // eslint-disable-line no-throw-literal
  }
}
