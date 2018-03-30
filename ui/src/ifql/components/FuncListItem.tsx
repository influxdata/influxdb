import React, {PureComponent} from 'react'

interface Props {
  name: string
  onAddNode: (name: string) => void
}

export default class FuncListItem extends PureComponent<Props> {
  public render() {
    const {name} = this.props

    return (
      <li onClick={this.handleClick} className="dropdown-item func">
        <a>{name}</a>
      </li>
    )
  }

  private handleClick = () => {
    this.props.onAddNode(this.props.name)
  }
}
