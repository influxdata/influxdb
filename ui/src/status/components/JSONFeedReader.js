import React, {PropTypes} from 'react'

import FancyScrollbar from 'shared/components/FancyScrollbar'

const JSONFeedReader = ({data}) =>
  <FancyScrollbar>
    {data
      ? data.items.map(({date_published, title, content_html}, i) =>
          <div key={i}>
            <span>{`${new Date(date_published)}`}</span><h6>{title}</h6>
            <div dangerouslySetInnerHTML={{__html: content_html}} />
          </div>
        )
      : null}
  </FancyScrollbar>

const {shape} = PropTypes

// TODO: define JSONFeed schema
JSONFeedReader.propTypes = {
  data: shape().isRequired,
}

export default JSONFeedReader
