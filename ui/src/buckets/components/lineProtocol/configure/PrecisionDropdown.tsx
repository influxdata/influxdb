// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {WritePrecision} from 'src/types'
import {Precision} from 'src/types/dataLoaders'

interface Props {
  setPrecision: (precision: WritePrecision) => void
  precision: WritePrecision
}

const writePrecisions: WritePrecision[] = [
  WritePrecision.Ns,
  WritePrecision.Us,
  WritePrecision.Ms,
  WritePrecision.S,
]

const makePrecisionReadable = {
  [WritePrecision.Ns]: Precision.Nanoseconds,
  [WritePrecision.Us]: Precision.Microseconds,
  [WritePrecision.S]: Precision.Seconds,
  [WritePrecision.Ms]: Precision.Milliseconds,
}

class PrecisionDropdown extends PureComponent<Props> {
  public render() {
    const {setPrecision, precision} = this.props
    return (
      <Dropdown
        testID="wizard-step--lp-precision--dropdown"
        style={{width: '220px'}}
        button={(active, onClick) => (
          <Dropdown.Button active={active} onClick={onClick}>
            {`Precision: ${makePrecisionReadable[precision]}`}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse}>
            {writePrecisions.map(value => (
              <Dropdown.Item
                key={value}
                value={value}
                id={value}
                onClick={setPrecision}
                testID={`wizard-step--lp-precision-${value}`}
                selected={`${value}` === `${precision}`}
              >
                {makePrecisionReadable[value]}
              </Dropdown.Item>
            ))}
          </Dropdown.Menu>
        )}
      />
    )
  }
}

export default PrecisionDropdown
