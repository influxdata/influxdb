// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'

// Components
import {ClickOutside} from 'src/shared/components/ClickOutside'
import DropdownDivider from 'src/clockface/components/dropdowns/DropdownDivider'
import DropdownItem from 'src/clockface/components/dropdowns/DropdownItem'
import DropdownButton from 'src/clockface/components/dropdowns/DropdownButton'
import DapperScrollbars from 'src/shared/components/dapperScrollbars/DapperScrollbars'
import WaitingText from 'src/shared/components/WaitingText'

// Types
import {
  ComponentStatus,
  ComponentColor,
  ComponentSize,
  IconFont,
} from '@influxdata/clockface'
import {DropdownMenuColors} from 'src/clockface/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

export enum DropdownMode {
  ActionList = 'action',
  Radio = 'radio',
}

interface ThumbColors {
  start: string
  stop: string
}

interface PassedProps {
  children: JSX.Element[]
  onChange: (value: any) => void
  selectedID?: string
  widthPixels?: number
  menuWidthPixels?: number
  menuHeader?: JSX.Element
  icon?: IconFont
  customClass?: string
}

export interface DefaultProps {
  buttonColor?: ComponentColor
  buttonSize?: ComponentSize
  status?: ComponentStatus
  maxMenuHeight?: number
  menuColor?: DropdownMenuColors
  mode?: DropdownMode
  titleText?: string
  wrapMenuText?: boolean
  testID?: string
  buttonTestID?: string
}

export type Props = PassedProps & DefaultProps

interface State {
  expanded: boolean
}

@ErrorHandling
class Dropdown extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    buttonColor: ComponentColor.Default,
    buttonSize: ComponentSize.Small,
    status: ComponentStatus.Default,
    maxMenuHeight: 250,
    menuColor: DropdownMenuColors.Sapphire,
    mode: DropdownMode.Radio,
    titleText: '',
    wrapMenuText: false,
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
      customClass,
      mode,
      wrapMenuText,
    } = this.props

    return classnames(
      `dropdown dropdown-${buttonSize} dropdown-${buttonColor}`,
      {
        disabled: status === ComponentStatus.Disabled,
        'dropdown-wrap': wrapMenuText,
        'dropdown-truncate': !wrapMenuText,
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
      widthPixels,
      menuWidthPixels,
      menuHeader,
      menuColor,
      children,
      testID,
    } = this.props

    const {expanded} = this.state

    if (!expanded) {
      return null
    }

    let width = '100%'

    if (widthPixels) {
      width = `${widthPixels}px`
    }

    if (menuWidthPixels) {
      width = `${menuWidthPixels}px`
    }

    const {start, stop} = this.thumbColorsFromTheme

    return (
      <div
        className={`dropdown--menu-container dropdown--${menuColor}`}
        style={{width}}
      >
        <DapperScrollbars
          style={{
            maxWidth: '100%',
            maxHeight: `${maxMenuHeight}px`,
          }}
          autoSize={true}
          autoHide={false}
          thumbStartColor={start}
          thumbStopColor={stop}
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
        </DapperScrollbars>
      </div>
    )
  }

  private get thumbColorsFromTheme(): ThumbColors {
    const {menuColor} = this.props

    switch (menuColor) {
      case DropdownMenuColors.Amethyst:
      case DropdownMenuColors.Sapphire:
        return {
          start: '#BEF0FF',
          stop: '#6BDFFF',
        }
      case DropdownMenuColors.Malachite:
        return {
          start: '#BEF0FF',
          stop: '#A5F3B4',
        }
      default:
      case DropdownMenuColors.Onyx:
        return {
          start: '#22ADF6',
          stop: '#9394FF',
        }
    }
  }

  private handleItemClick = (value: any): void => {
    const {onChange} = this.props
    onChange(value)
    this.collapseMenu()
  }
}

export default Dropdown
