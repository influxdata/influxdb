import React, {Component} from 'react'

const ManualRefresh = WrappedComponent =>
  class extends Component {
    constructor(props) {
      super(props)
      this.state = {
        manualRefresh: Date.now(),
      }
    }

    handleManualRefresh = () => {
      this.setState({
        manualRefresh: Date.now(),
      })
    }

    render() {
      return (
        <WrappedComponent
          {...this.props}
          manualRefresh={this.state.manualRefresh}
          onManualRefresh={this.handleManualRefresh}
        />
      )
    }
  }

export default ManualRefresh
