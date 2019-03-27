// Libraries
import React, {Component, CSSProperties, MouseEvent} from 'react'
import classnames from 'classnames'

// Components
import {ClickOutside} from 'src/shared/components/ClickOutside'
import DropdownDivider from 'src/clockface/components/dropdowns/DropdownDivider'
import DropdownItem from 'src/clockface/components/dropdowns/DropdownItem'
import DropdownButton from 'src/clockface/components/dropdowns/DropdownButton'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import WaitingText from 'src/shared/components/WaitingText'

// Types
import {
  DropdownMenuColors,
  ComponentStatus,
  ComponentColor,
  ComponentSize,
  IconFont,
} from '@influxdata/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

export enum DropdownMode {
  ActionList = 'action',
  Radio = 'radio',
}

interface OwnProps {
  children: JSX.Element[]
  onChange: (value: any) => void
}

interface DefaultProps {
  buttonTestID?: string
  selectedID?: string
  buttonColor?: ComponentColor
  buttonSize?: ComponentSize
  menuColor?: DropdownMenuColors
  status?: ComponentStatus
  widthPixels?: number
  icon?: IconFont
  wrapText?: boolean
  customClass?: string
  maxMenuHeight?: number
  mode?: DropdownMode
  titleText?: string
  menuHeader?: JSX.Element
  testID?: string
}

export type Props = OwnProps & DefaultProps

interface State {
  expanded: boolean
}

@ErrorHandling
class Dropdown extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    buttonColor: ComponentColor.Default,
    buttonSize: ComponentSize.Small,
    status: ComponentStatus.Default,
    wrapText: false,
    maxMenuHeight: 250,
    menuColor: DropdownMenuColors.Sapphire,
    mode: DropdownMode.Radio,
    titleText: '',
    testID: 'dropdown',
    buttonTestID: 'dropdown-button',
  }

  public static Button = DropdownButton
  public static Item = DropdownItem
  public static Divider = DropdownDivider

  constructor(props: Props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
    const {widthPixels} = this.props
    const width = widthPixels ? `${widthPixels}px` : '100%'

    return (
      <ClickOutside onClickOutside={this.collapseMenu}>
        <div className={this.containerClassName} style={{width}}>
          {this.button}
          {this.menuItems}
        </div>
      </ClickOutside>
    )
  }

  private toggleMenu = (e: MouseEvent<HTMLElement>): void => {
    e.preventDefault()
    this.setState({expanded: !this.state.expanded})
  }

  private collapseMenu = (): void => {
    this.setState({expanded: false})
  }

  private get containerClassName(): string {
    const {
      buttonColor,
      buttonSize,
      status,
      wrapText,
      customClass,
      mode,
    } = this.props

    return classnames(
      `dropdown dropdown-${buttonSize} dropdown-${buttonColor}`,
      {
        disabled: status === ComponentStatus.Disabled,
        'dropdown-wrap': wrapText,
        [customClass]: customClass,
        [`dropdown--${mode}`]: mode,
      }
    )
  }

  private get button(): JSX.Element {
    const {
      selectedID,
      status,
      buttonColor,
      buttonSize,
      icon,
      mode,
      titleText,
      buttonTestID,
    } = this.props

    const {expanded} = this.state
    const children: JSX.Element[] = this.props.children

    const selectedChild = children.find(child => child.props.id === selectedID)
    const isLoading = status === ComponentStatus.Loading

    let resolvedStatus = status
    let dropdownLabel

    if (isLoading) {
      dropdownLabel = <WaitingText text="Loading" />
    } else if (selectedChild) {
      dropdownLabel = selectedChild.props.children
    } else if (mode === DropdownMode.ActionList) {
      dropdownLabel = titleText
    } else {
      dropdownLabel = titleText
      resolvedStatus = ComponentStatus.Disabled
    }

    return (
      <DropdownButton
        active={expanded}
        color={buttonColor}
        size={buttonSize}
        icon={icon}
        onClick={this.toggleMenu}
        status={resolvedStatus}
        title={titleText}
        testID={buttonTestID}
      >
        {dropdownLabel}
      </DropdownButton>
    )
  }

  private get menuItems(): JSX.Element {
    const {
      selectedID,
      maxMenuHeight,
      menuHeader,
      menuColor,
      children,
      testID,
    } = this.props

    const {expanded} = this.state

    if (!expanded) {
      return null
    }

    return (
      <div
        className={`dropdown--menu-container dropdown--${menuColor}`}
        style={this.menuStyle}
      >
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={maxMenuHeight}
        >
          <div
            className="dropdown--menu"
            data-testid={`dropdown--menu ${testID}`}
          >
            {menuHeader && menuHeader}
            {React.Children.map(children, (child: JSX.Element) => {
              if (child.type === DropdownItem) {
                return (
                  <DropdownItem
                    {...child.props}
                    key={child.props.id}
                    selected={child.props.id === selectedID}
                    onClick={this.handleItemClick}
                  >
                    {child.props.children}
                  </DropdownItem>
                )
              } else if (child.type === DropdownDivider) {
                return <DropdownDivider {...child.props} key={child.props.id} />
              } else {
                throw new Error(
                  'Expected children of type <Dropdown.Item /> or <Dropdown.Divider />'
                )
              }
            })}
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get menuStyle(): CSSProperties {
    const {wrapText, widthPixels} = this.props

    let containerWidth = '100%'

    if (widthPixels) {
      containerWidth = `${widthPixels}px`
    }

    if (wrapText && widthPixels) {
      return {
        width: containerWidth,
      }
    }

    return {
      minWidth: containerWidth,
    }
  }

  private handleItemClick = (value: any): void => {
    const {onChange} = this.props
    onChange(value)
    this.collapseMenu()
  }
}

export default Dropdown
