// Libraries
import React, {PureComponent} from 'react'

import {ComponentStatus, Dropdown, SelectDropdown} from '@influxdata/clockface'
import {BuilderAggregateFunctionType} from 'src/types'

interface Props {
  options: string[]
  selectedOption: string
  testID: string
  onSelect?: (option: BuilderAggregateFunctionType) => void
  onDelete?: () => void
  isInCheckOverlay?: boolean
}

const emptyFunction = () => {}

export default class BuilderCardDropdownHeader extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--header',
  }

  public render() {
    const {
      children,
      isInCheckOverlay,
      options,
      onSelect,
      selectedOption,
      testID,
    } = this.props
    const button = () => (
      <Dropdown.Button status={ComponentStatus.Disabled} onClick={() => {}}>
        {selectedOption}
      </Dropdown.Button>
    )

    const menu = () => null
    return (
      <div className="builder-card--header" data-testid={testID}>
        {isInCheckOverlay ? (
          <Dropdown
            button={button}
            menu={menu}
            testID="disabled--filter-dropdown"
          />
        ) : (
          <SelectDropdown
            options={options}
            selectedOption={selectedOption}
            testID="select-option-dropdown"
            onSelect={onSelect ? onSelect : emptyFunction}
          />
        )}
        {children}
        {this.deleteButton}
      </div>
    )
  }

  private get deleteButton(): JSX.Element | undefined {
    const {onDelete} = this.props

    if (onDelete) {
      return <div className="builder-card--delete" onClick={onDelete} />
    }
  }
}
