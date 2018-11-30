// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import {ClickOutside} from 'src/shared/components/ClickOutside'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'

// Constants
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'

// Types
import {ColorLabel} from 'src/types/colors'

interface Props {
  selected: ColorLabel
  disabled?: boolean
  stretchToFit?: boolean
  colors: ColorLabel[]
  onChoose: (colors: ColorLabel) => void
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

  public state: State = {expanded: false}

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
    const {colors, selected} = this.props

    return (
      <div className="color-dropdown--menu">
        <FancyScrollbar
          autoHide={false}
          autoHeight={true}
          maxHeight={DROPDOWN_MENU_MAX_HEIGHT}
        >
          {colors.map((color, i) => (
            <div
              className={
                color.name === selected.name
                  ? 'color-dropdown--item active'
                  : 'color-dropdown--item'
              }
              key={i}
              onClick={this.handleColorClick(color)}
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
