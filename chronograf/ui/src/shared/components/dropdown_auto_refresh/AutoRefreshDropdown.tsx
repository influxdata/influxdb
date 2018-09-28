// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import {Dropdown, Button, ButtonShape, IconFont} from 'src/clockface'

// Constants
import autoRefreshOptions, {
  AutoRefreshOption,
  AutoRefreshOptionType,
} from 'src/shared/data/autoRefreshes'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  selected: number
  onChoose: (milliseconds: number) => void
  showManualRefresh?: boolean
  onManualRefresh?: () => void
}

@ErrorHandling
class AutoRefreshDropdown extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    showManualRefresh: true,
  }

  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
    }
  }

  public render() {
    return (
      <div className={this.className}>
        <Dropdown
          icon={this.dropdownIcon}
          widthPixels={this.dropdownWidthPixels}
          onChange={this.handleDropdownChange}
          selectedID={this.selectedID}
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
              <Dropdown.Item key={option.id} id={option.id} value={option}>
                {option.label}
              </Dropdown.Item>
            )
          })}
        </Dropdown>
        {this.manualRefreshButton}
      </div>
    )
  }

  public handleDropdownChange = (
    autoRefreshOption: AutoRefreshOption
  ): void => {
    const {onChoose} = this.props
    const {milliseconds} = autoRefreshOption

    onChoose(milliseconds)
  }

  private get isPaused(): boolean {
    const {selected} = this.props

    return selected === 0
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
      return 50
    }

    return 84
  }

  private get selectedID(): string {
    const {selected} = this.props
    const selectedOption = autoRefreshOptions.find(
      option => option.milliseconds === selected
    )

    return selectedOption.id
  }

  private get manualRefreshButton(): JSX.Element {
    const {showManualRefresh, onManualRefresh} = this.props

    if (!showManualRefresh) {
      return
    }

    if (this.isPaused) {
      return (
        <Button
          shape={ButtonShape.Square}
          icon={IconFont.Refresh}
          onClick={onManualRefresh}
        />
      )
    }

    return null
  }
}

export default AutoRefreshDropdown
