// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown} from 'src/clockface'

const timeUnits = ['Day', 'Hour', 'Minute']

const timeUnitStringToAbbreviation = {
  Day: 'd',
  Hour: 'h',
  Minute: 'm',
}

interface Props {
  onChange: (scheduleUnit: string, scheduleType: string) => void
  scheduleType: string
  selectedUnit: string
}

export default class TaskScheduleUnitDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        onChange={this.handleUnitChange}
        selectedID={this.props.selectedUnit}
      >
        {timeUnits.map(unit => {
          return (
            <Dropdown.Item
              value={timeUnitStringToAbbreviation[unit]}
              key={unit}
              id={timeUnitStringToAbbreviation[unit]}
            >
              {unit}
            </Dropdown.Item>
          )
        })}
      </Dropdown>
    )
  }

  private handleUnitChange = (unit: string) => {
    const {scheduleType} = this.props

    this.props.onChange(unit, scheduleType)
  }
}
