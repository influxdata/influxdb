import React, {Component} from 'react'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import classnames from 'classnames'
import {ErrorHandling} from 'src/shared/decorators/errors'

type MenuItemAction = () => void

export interface MenuItem {
  text: string
  action: MenuItemAction
  disabled?: boolean
}

interface Props {
  theme?: string
  icon: string
  informParent: () => void
  menuItems: MenuItem[]
}

interface State {
  expanded: boolean
}

@ErrorHandling
export default class MenuTooltipButton extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    theme: 'default',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
    const {expanded} = this.state

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={this.className} onClick={this.handleButtonClick}>
          {this.icon}
          {expanded && this.renderMenu}
        </div>
      </ClickOutside>
    )
  }

  private handleButtonClick = (): void => {
    const {informParent} = this.props

    this.setState({expanded: !this.state.expanded})
    informParent()
  }

  private handleMenuItemClick = (action: MenuItemAction) => (): void => {
    const {informParent} = this.props

    this.setState({expanded: false})
    action()
    informParent()
  }

  private handleClickOutside = (): void => {
    const {informParent} = this.props
    const {expanded} = this.state

    if (expanded === false) {
      return
    }

    this.setState({expanded: false})
    informParent()
  }

  private get className(): string {
    const {expanded} = this.state

    return classnames('dash-graph-context--button', {active: expanded})
  }

  private get icon(): JSX.Element {
    const {icon} = this.props

    return <span className={`icon ${icon}`} />
  }

  private get renderMenu(): JSX.Element {
    const {menuItems, theme} = this.props
    const {expanded} = this.state

    if (expanded === false) {
      return null
    }

    return (
      <div className={`dash-graph-context--menu ${theme}`}>
        {menuItems.map((option, i) => (
          <div
            key={i}
            className={`dash-graph-context--menu-item${
              option.disabled ? ' disabled' : ''
            }`}
            onClick={
              option.disabled ? null : this.handleMenuItemClick(option.action)
            }
          >
            {option.text}
          </div>
        ))}
      </div>
    )
  }
}
