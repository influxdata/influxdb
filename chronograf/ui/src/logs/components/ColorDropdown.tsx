import React, {Component, MouseEvent} from 'react'

import classnames from 'classnames'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {DROPDOWN_MENU_MAX_HEIGHT} from 'src/shared/constants/index'
import {SEVERITY_COLORS, SeverityColorOptions} from 'src/logs/constants'

import {SeverityColor} from 'src/types/logs'

interface Props {
  selected: SeverityColor
  disabled?: boolean
  stretchToFit?: boolean
  onChoose: (severityLevel: string, colors: SeverityColor) => void
  severityLevel: string
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
              data-tag-key={color.name}
              data-tag-value={color.hex}
              key={i}
              onClick={this.handleColorClick}
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

  private handleColorClick = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement
    const hex = target.dataset.tagValue || target.parentElement.dataset.tagValue
    const nameString =
      target.dataset.tagKey || target.parentElement.dataset.tagKey
    const name = SeverityColorOptions[nameString]

    const color: SeverityColor = {name, hex}
    this.props.onChoose(this.props.severityLevel, color)
    this.setState({expanded: false})
  }
}
