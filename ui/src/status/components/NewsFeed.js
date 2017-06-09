import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {getJSONFeedAsync} from 'src/status/actions'

class NewsFeed extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {isFirstFetch, isFetching, isFailed, data} = this.props

    // TODO: Use AutoRefresh style 'initialFetch' + spinner approach
    if (isFirstFetch && isFetching) {
      return (
        // TODO: Factor this out of here and AutoRefresh
        <div className="graph-fetching">
          <div className="graph-spinner" />
        </div>
      )
    }

    if (isFailed) {
      return isFirstFetch
        ? <span>Failed to load NewsFeed.</span>
        : <div>
            <span>Failed to refresh NewsFeed</span><div data={data} />
          </div>
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
        <div data={data} />
      </div>
    )
  }

  componentDidMount() {
    const {source, getJSONFeed} = this.props
    getJSONFeed(source.links.status)
  }
}

const {bool, func, shape, string} = PropTypes

NewsFeed.propTypes = {
  source: shape({
    links: shape({
      status: string.isRequired,
    }).isRequired,
  }).isRequired,
  isFirstFetch: bool.isRequired,
  isFetching: bool.isRequired,
  isFailed: bool.isRequired,
  data: shape(),
  getJSONFeed: func.isRequired,
}

const mapStateToProps = ({
  JSONFeed: {isFirstFetch, isFetching, isFailed, data},
}) => ({
  isFirstFetch,
  isFetching,
  isFailed,
  data,
})

const mapDispatchToProps = dispatch => ({
  getJSONFeed: bindActionCreators(getJSONFeedAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(NewsFeed)
