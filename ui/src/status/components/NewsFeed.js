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
  isFetching: bool.isRequired,
  isFailed: bool.isRequired,
  data: shape(),
  getJSONFeed: func.isRequired,
}

const mapStateToProps = ({JSONFeed: {isFetching, isFailed, data}}) => ({
  isFetching,
  isFailed,
  data,
})

const mapDispatchToProps = dispatch => ({
  getJSONFeed: bindActionCreators(getJSONFeedAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(NewsFeed)
