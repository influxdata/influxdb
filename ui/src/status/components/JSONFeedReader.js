import React, {PropTypes} from 'react'

import moment from 'moment'

const JSONFeedReader = ({data}) =>
  data
    ? <div className="newsfeed">
        {data.items.map(({date_published, title, content_html}, i) =>
          <div key={i} className="newsfeed--post">
            <div className="newsfeed--date">
              {`${moment(date_published).format('MMM DD')}`}
            </div>
            <div className="newsfeed--post-title">{title}</div>
            <div className="newsfeed--content">
              <div dangerouslySetInnerHTML={{__html: content_html}} />
            </div>
          </div>
        )}
      </div>
    : null

const {shape} = PropTypes

// TODO: define JSONFeed schema
JSONFeedReader.propTypes = {
  data: shape().isRequired,
}

export default JSONFeedReader
