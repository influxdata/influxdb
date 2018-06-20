import AJAX from 'src/utils/ajax'
import {JSONFeedData} from 'src/types'

const excludeBasepath = true // don't prefix route of external link with basepath/

export const fetchJSONFeed = (url: string) =>
  AJAX<JSONFeedData>(
    {
      method: 'GET',
      url,
      // For explanation of why this header makes this work:
      // https://stackoverflow.com/questions/22968406/how-to-skip-the-options-preflight-request-in-angularjs
      headers: {'Content-Type': 'text/plain; charset=UTF-8'},
    },
    excludeBasepath // don't prefix route of external link with basepath
  )
