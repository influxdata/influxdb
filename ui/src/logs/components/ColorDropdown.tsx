import React, {Component} from 'react'

import classnames from 'classnames'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {SEVERITY_COLORS} from 'src/logs/constants'

export interface Color {
  hex: string
  name: string
}

interface Props {
  selected: Color
  disabled?: boolean
  stretchToFit?: boolean
  onChoose: (colors: Color) => void
}

interface State {
  expanded: boolean
}

@ErrorHandling
export default class ColorDropdown extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    stretchToFit: false,
    disabled: false,
  }

  constructor(props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
    const {expanded} = this.state
    const {selected} = this.props

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={this.dropdownClassNames}>
          <div
            className={this.buttonClassNames}
            onClick={this.handleToggleMenu}
          >
            <div
              className="color-dropdown--swatch"
              style={{backgroundColor: selected.hex}}
            />
            <div className="color-dropdown--name">{selected.name}</div>
            <span className="caret" />
          </div>
          {expanded && this.renderMenu}
        </div>
      </ClickOutside>
    )
  }

  private get dropdownClassNames(): string {
    const {stretchToFit} = this.props
    const {expanded} = this.state

    return classnames('color-dropdown', {
      open: expanded,
      'color-dropdown--stretch': stretchToFit,
    })
  }

  private get buttonClassNames(): string {
    const {disabled} = this.props
    const {expanded} = this.state

    return classnames('btn btn-sm btn-default color-dropdown--toggle', {
      active: expanded,
      'color-dropdown__disabled': disabled,
    })
  }

  private get renderMenu(): JSX.Element {
    const {selected} = this.props

    return (
      <div className="color-dropdown--menu">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
        >
          {SEVERITY_COLORS.map((color, i) => (
            <div
              className={classnames('color-dropdown--item', {
                active: color.name === selected.name,
              })}
              key={i}
              onClick={this.handleColorClick(color)}
              title={color.name}
            >
              <span
                className="color-dropdown--swatch"
                style={{backgroundColor: color.hex}}
              />
              <span className="color-dropdown--name">{color.name}</span>
            </div>
          ))}
        </FancyScrollbar>
      </div>
    )
  }

  private handleToggleMenu = (): void => {
    const {disabled} = this.props

    if (disabled) {
      return
    }
    this.setState({expanded: !this.state.expanded})
  }

  private handleClickOutside = (): void => {
    this.setState({expanded: false})
  }

  private handleColorClick = color => (): void => {
    this.props.onChoose(color)
    this.setState({expanded: false})
  }
}
