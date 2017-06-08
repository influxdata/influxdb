import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {getJSONFeedAsync} from 'src/status/actions'

import {errorThrown as errorThrownAction} from 'shared/actions/errors'

const url = 'https://daringfireball.net/feeds/json'

const withIntervalPolling = ComposedComponent => {
  return class extends Component {
    constructor(props) {
      super(props)

      this.state = {
        isFetching: false,
      }
    }

    async componentWillReceiveProps() {
      this.setState({isFetching: true})
      await getJSONFeedAsync(url)
      this.setState({isFetching: false})
    }

    shouldComponentUpdate(nextProps) {
      // only update if data updated, a la AutoRefresh
    }

    render() {
      return <ComposedComponent data={data} {...this.props} />
    }
  }
}

// don't store in redux state?
const mapStateToProps = ({status: {JSONFeed}}) => ({
  JSONFeed,
  // throw error from thunk and put errorThrown here
})

const mapDispatchToProps = dispatch => ({
  errorThrown: bindActionCreators(errorThrownAction, dispatch),
})

export default connect(null, mapDispatchToProps)(withIntervalPolling)
