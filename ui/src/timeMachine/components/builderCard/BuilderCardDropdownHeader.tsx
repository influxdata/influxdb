// Libraries
import React, {PureComponent} from 'react'

import {SelectDropdown} from '@influxdata/clockface'
import {BuilderAggregateFunctionType} from 'src/types'

interface Props {
  options: string[]
  selectedOption: string
  testID: string
  onSelect?: (option: BuilderAggregateFunctionType) => void
  onDelete?: () => void
}

const emptyFunction = () => {}

export default class BuilderCardDropdownHeader extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--header',
  }

  public render() {
    const {children, options, onSelect, selectedOption, testID} = this.props

    return (
      <div className="builder-card--header" data-testid={testID}>
        <SelectDropdown
          options={options}
          selectedOption={selectedOption}
          testID="select-option-dropdown"
          onSelect={onSelect ? onSelect : emptyFunction}
        />

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
