import axios from 'axios';

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
    if (window.basepath) {
      url = `${window.basepath}${url}`
    }

    if (resource) {
      if (!links) {
        const linksRes = await axios({
          url: '/chronograf/v1/',
          method: 'GET',
        })
        links = linksRes.data
      }
      url = id ? `${links[resource]}/${id}` : links[resource]
    }

    return axios({
      url,
      method,
      data,
      params,
      headers,
    })
  }
  catch (error) {
    console.error(error)
  }
}
