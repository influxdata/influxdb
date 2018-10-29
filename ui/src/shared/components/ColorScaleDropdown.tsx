import React, {Component, CSSProperties} from 'react'
import uuid from 'uuid'
import classnames from 'classnames'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

import {Color} from 'src/types/colors'
import {LINE_COLOR_SCALES} from 'src/shared/constants/graphColorPalettes'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onChoose: (colors: Color[]) => void
  stretchToFit?: boolean
  disabled?: boolean
  selected: Color[]
}

interface State {
  expanded: boolean
}

@ErrorHandling
export default class ColorScaleDropdown extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    disabled: false,
    stretchToFit: false,
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
        <div className={this.dropdownClassName}>
          <div className={this.buttonClassName} onClick={this.handleToggleMenu}>
            <div
              className="color-dropdown--swatches"
              style={this.generateGradientStyle(selected)}
            />
            <div className="color-dropdown--name">{selected[0].name}</div>
            <span className="caret" />
          </div>
          {expanded && this.renderMenu}
        </div>
      </ClickOutside>
    )
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
          {LINE_COLOR_SCALES.map(colorScale => (
            <div
              className={
                colorScale.name === selected[0].name
                  ? 'color-dropdown--item active'
                  : 'color-dropdown--item'
              }
              key={uuid.v4()}
              onClick={this.handleDropdownClick(colorScale)}
            >
              <div
                className="color-dropdown--swatches"
                style={this.generateGradientStyle(colorScale.colors)}
              />
              <span className="color-dropdown--name">{colorScale.name}</span>
            </div>
          ))}
        </FancyScrollbar>
      </div>
    )
  }

  private get buttonClassName(): string {
    const {disabled} = this.props
    const {expanded} = this.state

    return classnames('btn btn-sm btn-default color-dropdown--toggle', {
      active: expanded,
      'color-dropdown__disabled': disabled,
    })
  }

  private get dropdownClassName(): string {
    const {stretchToFit} = this.props
    const {expanded} = this.state

    return classnames('color-dropdown', {
      open: expanded,
      'color-dropdown--stretch': stretchToFit,
    })
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

  private handleDropdownClick = colorScale => (): void => {
    this.props.onChoose(colorScale)
    this.setState({expanded: false})
  }

  private generateGradientStyle = (colors): CSSProperties => ({
    background: `linear-gradient(to right, ${colors[0].hex} 0%,${
      colors[1].hex
    } 50%,${colors[2].hex} 100%)`,
  })
}
