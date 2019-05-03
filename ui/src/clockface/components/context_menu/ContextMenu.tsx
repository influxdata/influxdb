// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import ContextMenuItem from 'src/clockface/components/context_menu/ContextMenuItem'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import {Button} from '@influxdata/clockface'

// Types
import {
  ComponentColor,
  IconFont,
  ComponentSize,
  ButtonShape,
} from '@influxdata/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
  icon: IconFont
  onBoostZIndex?: (boostZIndex: boolean) => void
  text: string
  color: ComponentColor
  shape: ButtonShape
  testID: string
}

interface State {
  isExpanded: boolean
}

@ErrorHandling
class ContextMenu extends Component<Props, State> {
  public static defaultProps = {
    color: ComponentColor.Primary,
    shape: ButtonShape.Square,
    text: '',
    testID: 'context-menu',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {icon, text, shape, color, testID} = this.props

    return (
      <ClickOutside onClickOutside={this.handleCollapseMenu}>
        <div className="context-menu--container">
          <Button
            className={this.toggleClassName}
            onClick={this.handleExpandMenu}
            text={text}
            shape={shape}
            icon={icon}
            size={ComponentSize.ExtraSmall}
            color={color}
            testID={testID}
          />
          {this.menu}
        </div>
      </ClickOutside>
    )
  }

  private handleExpandMenu = (): void => {
    const {onBoostZIndex} = this.props

    if (onBoostZIndex) {
      onBoostZIndex(true)
    }

    this.setState({isExpanded: true})
  }

  private handleCollapseMenu = (): void => {
    const {onBoostZIndex} = this.props

    if (onBoostZIndex) {
      onBoostZIndex(false)
    }

    this.setState({isExpanded: false})
  }

  private get menu(): JSX.Element {
    const {children} = this.props

    return (
      <div className={this.menuClassName}>
        <div className={this.listClassName}>
          {React.Children.map(children, (child: JSX.Element) => {
            if (child.type === ContextMenuItem) {
              return (
                <ContextMenuItem
                  {...child.props}
                  onCollapseMenu={this.handleCollapseMenu}
                />
              )
            } else {
              return child
            }
          })}
        </div>
      </div>
    )
  }

  private get listClassName(): string {
    const {color} = this.props

    return classnames('context-menu--list', {
      [`context-menu--${color}`]: color,
    })
  }

  private get menuClassName(): string {
    const {isExpanded} = this.state

    return classnames('context-menu--list-container', {open: isExpanded})
  }

  private get toggleClassName(): string {
    const {color} = this.props
    const {isExpanded} = this.state

    return classnames('context-menu--toggle', {
      [`context-menu--${color}`]: color,
      active: isExpanded,
    })
  }
}

export default ContextMenu
