import React, {Component} from 'react'
import ReactDOM from 'react-dom'

export default function enhanceWithClickOutside(WrappedComponent) {
  return class extends Component {
    componentDidMount() {
      document.addEventListener('click', this.handleClickOutside, true)
    }

    componentWillUnmount() {
      document.removeEventListener('click', this.handleClickOutside, true)
    }

    handleClickOutside = e => {
      const domNode = ReactDOM.findDOMNode(this)
      if (
        (!domNode || !domNode.contains(e.target)) &&
        typeof this.wrappedComponent.handleClickOutside === 'function'
      ) {
        this.wrappedComponent.handleClickOutside(e)
      }
    }

    render() {
      return (
        <WrappedComponent
          {...this.props}
          ref={ref => (this.wrappedComponent = ref)}
        />
      )
    }
  }
}
