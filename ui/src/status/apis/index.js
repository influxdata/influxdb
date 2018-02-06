import AJAX from 'utils/ajax'

export const fetchJSONFeed = url =>
  AJAX(
    {
      method: 'GET',
      url,
      // For explanation of why this header makes this work:
      // https://stackoverflow.com/questions/22968406/how-to-skip-the-options-preflight-request-in-angularjs
      headers: {'Content-Type': 'text/plain; charset=UTF-8'},
    },
    {excludeBasepath: true} // don't prefix route of external link with basepath
  )
