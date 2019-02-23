// Libraries
import React, {PureComponent} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
  emptyState: JSX.Element
}

export default class ResourceListBody extends PureComponent<Props> {
  public render() {
    return <div className="resource-list--body">{this.children}</div>
  }

  private get children(): JSX.Element | JSX.Element[] {
    const {children, emptyState} = this.props

    if (React.Children.count(children) === 0) {
      return emptyState
    }

    return children
  }
}
