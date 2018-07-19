import React, {PureComponent} from 'react'
import classnames from 'classnames'

interface Props {
  name: string
  index: number
  onClickTab: (index: number) => void
  isActive: boolean
}

class TableTabItem extends PureComponent<Props> {
  public render() {
    return (
      <div className={this.className} onClick={this.handleClick}>
        {this.props.name}
      </div>
    )
  }

  private handleClick = (): void => {
    this.props.onClickTab(this.props.index)
  }

  get className(): string {
    return classnames('table--tab', {
      active: this.props.isActive,
    })
  }
}

export default TableTabItem
