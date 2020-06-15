// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import {
  SquareButton,
  IconFont,
  ComponentStatus,
  Dropdown,
} from '@influxdata/clockface'

// Constants
import autoRefreshOptions, {
  AutoRefreshOption,
  AutoRefreshOptionType,
} from 'src/shared/data/autoRefreshes'

// Types
import {AutoRefresh, AutoRefreshStatus} from 'src/types'
import {CLOUD} from 'src/shared/constants'

const DROPDOWN_WIDTH_COLLAPSED = 50
const DROPDOWN_WIDTH_FULL = 84

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selected: AutoRefresh
  onChoose: (milliseconds: number) => void
  showManualRefresh: boolean
  onManualRefresh?: () => void
}

@ErrorHandling
export default class AutoRefreshDropdown extends Component<Props> {
  public static defaultProps = {
    showManualRefresh: true,
  }

  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
    }
  }

  public render() {
    if (CLOUD) {
      return false
    }
    return (
      <div className={this.className}>
        <Dropdown
          style={{width: `${this.dropdownWidthPixels}px`}}
          button={(active, onClick) => (
            <Dropdown.Button
              active={active}
              onClick={onClick}
              status={this.dropdownStatus}
              icon={this.dropdownIcon}
            >
              {this.selectedOptionLabel}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu
              onCollapse={onCollapse}
              style={{width: `${DROPDOWN_WIDTH_FULL}px`}}
            >
              {autoRefreshOptions.map(option => {
                if (option.type === AutoRefreshOptionType.Header) {
                  return (
                    <Dropdown.Divider
                      key={option.id}
                      id={option.id}
                      text={option.label}
                    />
                  )
                }

                return (
                  <Dropdown.Item
                    key={option.id}
                    id={option.id}
                    value={option}
                    selected={option.id === this.selectedOptionID}
                    onClick={this.handleDropdownChange}
                  >
                    {option.label}
                  </Dropdown.Item>
                )
              })}
            </Dropdown.Menu>
          )}
        />
        {this.manualRefreshButton}
      </div>
    )
  }

  public handleDropdownChange = (
    autoRefreshOption: AutoRefreshOption
  ): void => {
    this.props.onChoose(autoRefreshOption.milliseconds)
  }

  private get dropdownStatus(): ComponentStatus {
    if (this.isDisabled) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get isDisabled(): boolean {
    const {selected} = this.props

    return selected.status === AutoRefreshStatus.Disabled
  }

  private get isPaused(): boolean {
    const {selected} = this.props

    return selected.status === AutoRefreshStatus.Paused || this.isDisabled
  }

  private get className(): string {
    return classnames('autorefresh-dropdown', {paused: this.isPaused})
  }

  private get dropdownIcon(): IconFont {
    if (this.isPaused) {
      return IconFont.Pause
    }

    return IconFont.Refresh
  }

  private get dropdownWidthPixels(): number {
    if (this.isPaused) {
      return DROPDOWN_WIDTH_COLLAPSED
    }

    return DROPDOWN_WIDTH_FULL
  }

  private get selectedOptionID(): string {
    const {selected} = this.props

    const selectedOption = autoRefreshOptions.find(
      option => option.milliseconds === selected.interval
    )

    return selectedOption.id
  }

  private get selectedOptionLabel(): string {
    const {selected} = this.props

    const selectedOption = autoRefreshOptions.find(
      option => option.milliseconds === selected.interval
    )

    return selectedOption.label
  }

  private get manualRefreshButton(): JSX.Element {
    const {showManualRefresh, onManualRefresh} = this.props

    if (!showManualRefresh) {
      return
    }

    if (this.isPaused) {
      return (
        <SquareButton
          icon={IconFont.Refresh}
          onClick={onManualRefresh}
          className="autorefresh-dropdown--pause"
        />
      )
    }

    return null
  }
}
