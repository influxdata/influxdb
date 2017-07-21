import React, {PropTypes} from 'react'

import moment from 'moment'

const JSONFeedReader = ({data}) =>
  data && data.items
    ? <div className="newsfeed">
        {data.items
          ? data.items.map(
              ({
                id,
                date_published,
                url,
                title,
                author: {name},
                image,
                content_text: contentText,
              }) =>
                <div key={id} className="newsfeed--post">
                  <div className="newsfeed--date">
                    {`${moment(date_published).format('MMM DD')}`}
                  </div>
                  <div className="newsfeed--post-title">
                    <a href={url} target="_blank">
                      <h6>
                        {title}
                      </h6>
                    </a>
                    <span>
                      by {name}
                    </span>
                  </div>
                  <div className="newsfeed--content">
                    {image ? <img src={image} /> : null}
                    <p>
                      {contentText}
                    </p>
                  </div>
                </div>
            )
          : null}
      </div>
    : null

const {arrayOf, shape, string} = PropTypes

JSONFeedReader.propTypes = {
  data: shape({
    items: arrayOf(
      shape({
        author: shape({
          name: string.isRequired,
        }).isRequired,
        content_text: string.isRequired,
        date_published: string.isRequired,
        id: string.isRequired,
        image: string,
        title: string.isRequired,
        url: string.isRequired,
      })
    ),
  }).isRequired,
}

export default JSONFeedReader
