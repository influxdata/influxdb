import {PureComponent} from 'react'

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
    return process.env.NODE_ENV !== 'development'
  }
}
