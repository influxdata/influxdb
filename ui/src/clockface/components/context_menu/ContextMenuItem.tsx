// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

interface Props {
  label: string
  action: (value?: any) => void
  description: string
  testID: string
  value?: any
  disabled?: boolean
  onCollapseMenu?: () => void
}

class ContextMenuItem extends Component<Props> {
  public static defaultProps = {
    description: '',
    testID: 'context-menu-item',
  }

  public render() {
    const {label, disabled, testID} = this.props

    return (
      <button
        className={this.className}
        onClick={this.handleClick}
        disabled={disabled}
        data-testid={testID}
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

    action(value)

    if (onCollapseMenu) {
      onCollapseMenu()
    }
  }
}

export default ContextMenuItem
