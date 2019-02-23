// Libraries
import React, {PureComponent} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

export default class ResourceListHeader extends PureComponent<Props> {
  public render() {
    const {children} = this.props

    return <div className="resource-list--header">{children}</div>
  }
}
