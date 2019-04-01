import {PureComponent} from 'react'

export default class extends PureComponent {
  render() {
    const {children} = this.props

    if (process.env.CLOUD !== 'true') {
      return children
    }

    return null
  }
}
