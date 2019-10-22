import {PureComponent} from 'react'
import {CLOUD} from 'src/shared/constants'

export default class extends PureComponent {
  render() {
    const {children} = this.props

    if (CLOUD === 'true') {
      return children
    }

    return null
  }
}
