// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

interface Props {
  label: string
  description?: string
  action: (value?: any) => void
  value?: any
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
        {this.description}
      </button>
    )
  }

  private get className(): string {
    const {disabled} = this.props

    return classnames('context-menu--item', {
      'context-menu--item__disabled': disabled,
    })
  }

  private get description(): JSX.Element {
    const {description} = this.props
    if (description) {
      return <div className="contex-menu--item-description">{description}</div>
    }
  }

  private handleClick = (): void => {
    const {action, onCollapseMenu, value} = this.props

    if (!onCollapseMenu) {
      return
    }

    onCollapseMenu()
    action(value)
  }
}

export default ContextMenuItem
