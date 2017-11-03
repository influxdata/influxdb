import axios from 'axios'

let links

// do not prefix route with basepath, ex. for external links
const addBasepath = (url, excludeBasepath) => {
  const basepath = window.basepath || ''

  return excludeBasepath ? url : `${basepath}${url}`
}

const generateResponseWithLinks = (response, newLinks) => {
  const {auth, logout, external, users, organizations} = newLinks
  return {
    ...response,
    auth: {links: auth},
    logoutLink: logout,
    external,
    users,
    organizations,
  }
}

const AJAX = async (
  {url, resource, id, method = 'GET', data = {}, params = {}, headers = {}},
  excludeBasepath
) => {
  try {
    let response

    url = addBasepath(url, excludeBasepath)

    if (!links) {
      const linksRes = (response = await axios({
        url: addBasepath('/chronograf/v1', excludeBasepath),
        method: 'GET',
      }))
      links = linksRes.data
    }

    if (resource) {
      url = id
        ? addBasepath(`${links[resource]}/${id}`, excludeBasepath)
        : addBasepath(`${links[resource]}`, excludeBasepath)
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
