import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  name: string
  onAddNode: (name: string) => void
  selectedFunc: string
  onSetSelectedFunc: (name: string) => void
}

@ErrorHandling
export default class FuncListItem extends PureComponent<Props> {
  public render() {
    return (
      <li
        onClick={this.handleClick}
        onMouseEnter={this.handleMouseEnter}
        className={`flux-func--item ${this.activeClass}`}
      >
        {this.props.name}
      </li>
    )
  }

  private get activeClass(): string {
    const {name, selectedFunc} = this.props
    return name === selectedFunc ? 'active' : ''
  }

  private handleMouseEnter = () => {
    this.props.onSetSelectedFunc(this.props.name)
  }

  private handleClick = () => {
    this.props.onAddNode(this.props.name)
  }
}
