import React, {PropTypes} from 'react'

const JSONFeedReader = ({data}) => {
  console.log('JSONFeedReader data', data)
  return (
    <div>
      <p>{JSON.stringify(data)}</p>
    </div>
  )
}

const {shape} = PropTypes

// TODO: define JSONFeed schema
JSONFeedReader.propTypes = {
  data: shape().isRequired,
}

export default JSONFeedReader
