// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

interface Props {
  label: string
  action: () => void
  onCollapseMenu?: () => void
  disabled?: boolean
}

class ContextMenuItem extends Component<Props> {
  public render() {
    const {label, disabled} = this.props

    return (
      <button
        className={this.className}
        onClick={this.handleClick}
        disabled={disabled}
      >
        {label}
      </button>
    )
  }

  private get className(): string {
    const {disabled} = this.props

    return classnames('context-menu--item', {
      'context-menu--item__disabled': disabled,
    })
  }

  private handleClick = (): void => {
    const {action, onCollapseMenu} = this.props

    if (!onCollapseMenu) {
      return
    }

    onCollapseMenu()
    action()
  }
}

export default ContextMenuItem
