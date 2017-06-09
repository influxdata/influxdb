import React, {PropTypes} from 'react'

import moment from 'moment'

const JSONFeedReader = ({data}) =>
  data
    ? <div className="row" style={{padding: '10px'}}>
        {data.items.map(({date_published, title, content_html}, i) =>
          <div key={i} style={{display: 'block'}}>
            <div className="col-sm-12">
              <div className="col-sm-3" style={{display: 'inline'}}>
                <span>{`${moment(date_published).format('MMM D')}`}</span>
              </div>
              <div className="col-sm-9" style={{display: 'inline'}}>
                <span><b>{title}</b></span>
                <div dangerouslySetInnerHTML={{__html: content_html}} />
              </div>
            </div>
            <div className="col-sm-12">
              <hr />
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
