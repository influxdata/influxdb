import React, {PropTypes} from 'react'

import {getJSONFeedAsync} from 'src/status/actions'

url

const JSONFeedPoller = async EnhancedComponent => {
  // TODO: use setState on fetch since otherwise need to track news feed by id in redux state
  try {
    const {data} = getJSONFeedAsync(url)
  }
  return <EnhancedComponent />
}

export default JSONFeedPoller
