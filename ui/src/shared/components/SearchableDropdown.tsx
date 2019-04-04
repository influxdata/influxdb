// Libraries
import React, {Component, ChangeEvent} from 'react'
import {omit} from 'lodash'

// Components
import {Input} from '@influxdata/clockface'
import Dropdown, {
  Props as DropdownProps,
} from 'src/clockface/components/dropdowns/Dropdown'

// Types
import {ComponentSize} from '@influxdata/clockface'
import {DropdownMenuColors} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends DropdownProps {
  searchTerm?: string
  searchPlaceholder?: string
  onChangeSearchTerm?: (value: string) => void
  buttonSize: ComponentSize
}

@ErrorHandling
export default class SearchableDropdown extends Component<Props> {
  public static defaultProps = {
    buttonSize: ComponentSize.Small,
  }

  public render() {
    const {searchTerm, searchPlaceholder, buttonSize, testID} = this.props

    const dropdownProps = omit(this.props, [
      'searchTerm',
      'searchPlaceholder',
      'onChangeSearch',
    ])

    return (
      <Dropdown
        {...dropdownProps}
        menuColor={DropdownMenuColors.Onyx}
        menuHeader={
          <Input
            customClass="searchable-dropdown--menu-input"
            onChange={this.handleChange}
            value={searchTerm}
            placeholder={searchPlaceholder}
            size={buttonSize}
            autoFocus={true}
          />
        }
        testID={testID}
      />
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onChangeSearchTerm} = this.props

    if (onChangeSearchTerm) {
      onChangeSearchTerm(e.target.value)
    }
  }
}
