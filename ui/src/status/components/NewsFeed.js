import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {getJSONFeedAsync} from 'src/status/actions'

class NewsFeed extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {isFetching, isFailed, data} = this.props

    if (isFetching) {
      return <span>Loading...</span>
    }

    if (isFailed) {
      return <span>Failed to load NewsFeed.</span>
    }
    console.log('data', data)
    return (
      <div>
        {data}
      </div>
    )
  }

  componentDidMount() {
    const {url, getJSONFeed} = this.props
    console.log('componentDidMount', url)
    if (url) {
      getJSONFeed(url)
    }
  }
}

const {bool, func, shape, string} = PropTypes

NewsFeed.propTypes = {
  isFetching: bool.isRequired,
  isFailed: bool.isRequired,
  data: shape(),
  url: string.isRequired,
  getJSONFeed: func.isRequired,
}

const mapStateToProps = ({
  JSONFeed: {isFetching, isFailed, data},
  auth: {
    me: {links: {jsonFeed: url = 'https://daringfireball.net/feeds/json'}},
  },
}) => ({
  isFetching,
  isFailed,
  data,
  url,
})

const mapDispatchToProps = dispatch => ({
  getJSONFeed: bindActionCreators(getJSONFeedAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(NewsFeed)
