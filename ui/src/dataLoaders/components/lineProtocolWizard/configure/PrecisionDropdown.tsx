// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {WritePrecision} from '@influxdata/influx'
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
      <div className={'wizard-step--footer dropdown'}>
        <label>Time Precision </label>
        <Dropdown
          selectedID={precision}
          onChange={setPrecision}
          widthPixels={200}
        >
          {writePrecisions.map(value => (
            <Dropdown.Item key={value} value={value} id={value}>
              {makePrecisionReadable[value]}
            </Dropdown.Item>
          ))}
        </Dropdown>
      </div>
    )
  }
}

export default PrecisionDropdown
