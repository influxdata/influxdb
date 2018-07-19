import React, {SFC} from 'react'
import {JSONFeedData} from 'src/types'

import moment from 'moment'

interface Props {
  data: JSONFeedData
}

const JSONFeedReader: SFC<Props> = ({data}) =>
  data && data.items ? (
    <div className="newsfeed">
      {data.items
        ? data.items.map(
            ({
              id,
              date_published: datePublished,
              url,
              title,
              author: {name},
              image,
              content_text: contentText,
            }) => (
              <div key={id} className="newsfeed--post">
                <div className="newsfeed--date">
                  {`${moment(datePublished).format('MMM DD')}`}
                </div>
                <div className="newsfeed--post-title">
                  <a href={url} target="_blank">
                    <h6>{title}</h6>
                  </a>
                  <span>by {name}</span>
                </div>
                <div className="newsfeed--content">
                  {image ? <img src={image} /> : null}
                  <p>{contentText}</p>
                </div>
              </div>
            )
          )
        : null}
    </div>
  ) : null

export default JSONFeedReader
