// Libraries
import {PureComponent} from 'react'

// Constants
import {CLOUD} from 'src/shared/constants'

interface Props {
  name?: string
}

export default class extends PureComponent<Props> {
  public render() {
    if (this.isHidden) {
      return null
    }

    return this.props.children
  }

  private get isHidden(): boolean {
    return CLOUD
  }
}
