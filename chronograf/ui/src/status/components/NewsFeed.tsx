import React, {Component} from 'react'
import {connect} from 'react-redux'

import {fetchJSONFeedAsync} from 'src/status/actions'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import JSONFeedReader from 'src/status/components/JSONFeedReader'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {JSONFeedData} from 'src/types'

interface Props {
  hasCompletedFetchOnce: boolean
  isFetching: boolean
  isFailed: boolean
  data: JSONFeedData
  fetchJSONFeed: (statusFeedURL: string) => void
  statusFeedURL: string
}

@ErrorHandling
class NewsFeed extends Component<Props> {
  public render() {
    const {hasCompletedFetchOnce, isFetching, isFailed, data} = this.props

    if (!hasCompletedFetchOnce) {
      return isFailed ? (
        <div className="graph-empty">
          <p>Failed to load News Feed</p>
        </div>
      ) : (
        <div className="graph-fetching">
          <div className="graph-spinner" />
        </div>
      )
    }

    return (
      <FancyScrollbar autoHide={false} className="newsfeed--container">
        {isFetching ? (
          <div className="graph-panel__refreshing">
            <div />
            <div />
            <div />
          </div>
        ) : null}
        {isFailed ? (
          <div className="graph-empty">
            <p>Failed to refresh News Feed</p>
          </div>
        ) : null}
        <JSONFeedReader data={data} />
      </FancyScrollbar>
    )
  }

  public componentDidMount() {
    const {statusFeedURL, fetchJSONFeed} = this.props

    fetchJSONFeed(statusFeedURL)
  }
}
const mstp = ({
  links: {
    external: {statusFeed: statusFeedURL},
  },
  JSONFeed: {hasCompletedFetchOnce, isFetching, isFailed, data},
}) => ({
  hasCompletedFetchOnce,
  isFetching,
  isFailed,
  data,
  statusFeedURL,
})

const mdtp = {
  fetchJSONFeed: fetchJSONFeedAsync,
}

export default connect(mstp, mdtp)(NewsFeed)
