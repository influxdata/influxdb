import axios from 'axios'

let links

const generateResponseWithLinks = (response, {auth, logout, external}) => ({
  ...response,
  auth: {links: auth},
  logoutLink: logout,
  external,
})

const AJAX = async ({
  url,
  resource,
  id,
  method = 'GET',
  data = {},
  params = {},
  headers = {},
}) => {
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

    return generateResponseWithLinks(response, links)
  } catch (error) {
    const {response} = error

    throw generateResponseWithLinks(response, links) // eslint-disable-line no-throw-literal
  }
}

export const get = async url => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export default AJAX
