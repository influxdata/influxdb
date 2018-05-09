import classnames from 'classnames'
import React, {PureComponent, MouseEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  tagKey: string
  tagValues: string[]
}

interface State {
  isOpen: boolean
}

@ErrorHandling
class TagListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  public render() {
    const {isOpen} = this.state

    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {this.tagItemLabel}
          <span className="ifql-schema-type">Tag Key</span>
        </div>
        {isOpen && this.renderTagValues}
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()
    this.setState({isOpen: !this.state.isOpen})
  }

  private get tagItemLabel(): string {
    const {tagKey} = this.props
    return `${tagKey}`
  }

  private get renderTagValues(): JSX.Element[] | JSX.Element {
    const {tagValues} = this.props
    if (!tagValues || !tagValues.length) {
      return <div className="ifql-schema-tree__empty">No tag values</div>
    }

    return tagValues.map(v => {
      return (
        <div key={v} className="ifql-schema-item readonly ifql-tree-node">
          {v}
        </div>
      )
    })
  }

  private get className(): string {
    const {isOpen} = this.state
    return classnames('ifql-schema-tree ifql-tree-node', {expanded: isOpen})
  }
}

export default TagListItem
