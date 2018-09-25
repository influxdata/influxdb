// Libraries
import React, {Component, CSSProperties, Fragment} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import {ClickOutside} from 'src/shared/components/ClickOutside'
import DropdownDivider from 'src/clockface/components/dropdowns/DropdownDivider'
import DropdownItem from 'src/clockface/components/dropdowns/DropdownItem'
import DropdownButton from 'src/clockface/components/dropdowns/DropdownButton'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Types
import {
  DropdownMenuColors,
  ComponentStatus,
  ComponentColor,
  ComponentSize,
  IconFont,
} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
  onChange: (selectedIDs: string[], value: any) => void
  onCollapse?: () => void
  selectedIDs: string[]
  buttonColor?: ComponentColor
  buttonSize?: ComponentSize
  menuColor?: DropdownMenuColors
  status?: ComponentStatus
  widthPixels?: number
  icon?: IconFont
  wrapText?: boolean
  customClass?: string
  maxMenuHeight?: number
  emptyText?: string
  separatorText?: string
}

interface State {
  expanded: boolean
}

@ErrorHandling
class MultiSelectDropdown extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    buttonColor: ComponentColor.Default,
    buttonSize: ComponentSize.Small,
    status: ComponentStatus.Default,
    wrapText: false,
    maxMenuHeight: 250,
    menuColor: DropdownMenuColors.Sapphire,
    emptyText: 'Choose an item',
    separatorText: ', ',
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
    this.validateChildCount()

    return (
      <ClickOutside onClickOutside={this.collapseMenu}>
        <div className={this.containerClassName} style={this.containerStyle}>
          {this.button}
          {this.menuItems}
        </div>
      </ClickOutside>
    )
  }

  private toggleMenu = (): void => {
    this.setState({expanded: !this.state.expanded})
  }

  private collapseMenu = (): void => {
    const {onCollapse} = this.props

    this.setState({expanded: false})

    if (onCollapse) {
      onCollapse()
    }
  }

  private get containerStyle(): CSSProperties {
    const {widthPixels} = this.props

    if (widthPixels) {
      return {width: `${widthPixels}px`}
    }

    return {width: '100%'}
  }

  private get containerClassName(): string {
    const {buttonColor, buttonSize, status, wrapText, customClass} = this.props

    return classnames(
      `dropdown dropdown-${buttonSize} dropdown-${buttonColor}`,
      {
        disabled: status === ComponentStatus.Disabled,
        'dropdown-wrap': wrapText,
        [customClass]: customClass,
      }
    )
  }

  private get button(): JSX.Element {
    const {
      selectedIDs,
      status,
      buttonColor,
      buttonSize,
      icon,
      children,
      emptyText,
      separatorText,
    } = this.props
    const {expanded} = this.state

    const selectedChildren = children.filter(child =>
      _.includes(selectedIDs, child.props.id)
    )

    let label: string | Array<string | JSX.Element>

    if (selectedChildren.length) {
      label = selectedChildren.map((sc, i) => {
        if (i < selectedChildren.length - 1) {
          return (
            <Fragment key={sc.props.id}>
              {sc.props.children}
              {separatorText}
            </Fragment>
          )
        }

        return sc.props.children
      })
    } else {
      label = emptyText
    }

    return (
      <DropdownButton
        active={expanded}
        color={buttonColor}
        size={buttonSize}
        icon={icon}
        onClick={this.toggleMenu}
        status={status}
      >
        {label}
      </DropdownButton>
    )
  }

  private get menuItems(): JSX.Element {
    const {selectedIDs, maxMenuHeight, menuColor, children} = this.props
    const {expanded} = this.state

    if (expanded) {
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
            <div className="dropdown--menu" data-test="dropdown-menu">
              {React.Children.map(children, (child: JSX.Element) => {
                if (this.childTypeIsValid(child)) {
                  if (child.type === DropdownItem) {
                    return (
                      <DropdownItem
                        {...child.props}
                        key={child.props.id}
                        checkbox={true}
                        selected={_.includes(selectedIDs, child.props.id)}
                        onClick={this.handleItemClick}
                      >
                        {child.props.children}
                      </DropdownItem>
                    )
                  }

                  return (
                    <DropdownDivider {...child.props} key={child.props.id} />
                  )
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

    return null
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
    const {onChange, selectedIDs} = this.props
    let updatedSelection

    if (_.includes(selectedIDs, value.id)) {
      updatedSelection = selectedIDs.filter(id => id !== value.id)
    } else {
      updatedSelection = [...selectedIDs, value.id]
    }

    onChange(updatedSelection, value)
  }

  private validateChildCount = (): void => {
    const {children} = this.props

    if (React.Children.count(children) === 0) {
      throw new Error(
        'Dropdowns require at least 1 child element. We recommend using Dropdown.Item and/or Dropdown.Divider.'
      )
    }
  }

  private childTypeIsValid = (child: JSX.Element): boolean =>
    child.type === DropdownItem || child.type === DropdownDivider
}

export default MultiSelectDropdown
