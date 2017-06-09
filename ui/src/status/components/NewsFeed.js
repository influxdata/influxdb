import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {fetchJSONFeedAsync} from 'src/status/actions'

import JSONFeedReader from 'src/status/components/JSONFeedReader'

class NewsFeed extends Component {
  constructor(props) {
    super(props)
  }

  // TODO: implement shouldComponentUpdate based on fetching conditions

  render() {
    const {hasCompletedFetchOnce, isFetching, isFailed, data} = this.props

    if (!hasCompletedFetchOnce) {
      return !isFailed
        ? // TODO: Factor this out of here and AutoRefresh
          <div className="graph-fetching">
            <div className="graph-spinner" />
          </div>
        : <span>Failed to load NewsFeed.</span>
    }

    if (isFailed) {
      return (
        <div>
          <span>Failed to refresh NewsFeed</span>
          <JSONFeedReader data={data} />
        </div>
      )
    }

    return (
      <div>
        {isFetching
          ? // TODO: Factor this out of here and AutoRefresh
            <div className="graph-panel__refreshing">
              <div />
              <div />
              <div />
            </div>
          : null}
        <JSONFeedReader data={data} />
      </div>
    )
  }

  // TODO: implement interval polling a la AutoRefresh
  componentDidMount() {
    const {source, fetchJSONFeed} = this.props

    fetchJSONFeed(source.links.status)
  }
}

const {bool, func, shape, string} = PropTypes

NewsFeed.propTypes = {
  source: shape({
    links: shape({
      status: string.isRequired,
    }).isRequired,
  }).isRequired,
  hasCompletedFetchOnce: bool.isRequired,
  isFetching: bool.isRequired,
  isFailed: bool.isRequired,
  data: shape(),
  fetchJSONFeed: func.isRequired,
}

const mapStateToProps = ({
  JSONFeed: {hasCompletedFetchOnce, isFetching, isFailed, data},
}) => ({
  hasCompletedFetchOnce,
  isFetching,
  isFailed,
  data,
})

const mapDispatchToProps = dispatch => ({
  fetchJSONFeed: bindActionCreators(fetchJSONFeedAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(NewsFeed)
