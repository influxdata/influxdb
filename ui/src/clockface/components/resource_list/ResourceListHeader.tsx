// Libraries
import React, {PureComponent} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
  filterComponent?: () => JSX.Element
}

export default class ResourceListHeader extends PureComponent<Props> {
  public render() {
    const {children} = this.props

    return (
      <div className="resource-list--header">
        {this.filter}
        <div className="resource-list--sorting">{children}</div>
      </div>
    )
  }

  private get filter(): JSX.Element {
    const {filterComponent} = this.props

    if (filterComponent) {
      return <div className="resource-list--filter">{filterComponent()}</div>
    }

    return <div className="resource-list--filter" />
  }
}
