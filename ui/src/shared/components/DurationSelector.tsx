// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

// Utils
import {areDurationsEqual} from 'src/shared/utils/duration'

export interface DurationOption {
  duration: string
  displayText: string
}

interface Props {
  selectedDuration: string
  onSelectDuration: (duration: string) => any
  durations: DurationOption[]
  disabled?: boolean
}

const DurationSelector: FunctionComponent<Props> = ({
  selectedDuration,
  onSelectDuration,
  durations,
  disabled = false,
}) => {
  let resolvedDurations = durations
  let selected: DurationOption = durations.find(
    d =>
      selectedDuration === d.duration ||
      areDurationsEqual(selectedDuration, d.duration)
  )

  if (!selected) {
    selected = {duration: selectedDuration, displayText: selectedDuration}
    resolvedDurations = [selected, ...resolvedDurations]
  }

  return (
    <Dropdown
      testID="duration-selector"
      button={(active, onClick) => (
        <Dropdown.Button
          testID="duration-selector--button"
          active={active}
          onClick={onClick}
          status={getStatus(disabled)}
        >
          {selected.displayText}
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu onCollapse={onCollapse} testID="duration-selector--menu">
          {resolvedDurations.map(({duration, displayText}) => (
            <Dropdown.Item
              id={duration}
              key={duration}
              value={duration}
              testID={`duration-selector--${duration}`}
              selected={duration === selectedDuration}
              onClick={onSelectDuration}
            >
              {displayText}
            </Dropdown.Item>
          ))}
        </Dropdown.Menu>
      )}
    />
  )
}

const getStatus = (disabled: boolean): ComponentStatus => {
  if (disabled) {
    return ComponentStatus.Disabled
  }

  return ComponentStatus.Default
}

export default DurationSelector
