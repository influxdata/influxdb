// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown} from 'src/clockface'

// Types
import {WritePrecision} from 'src/api'
import {Precision} from 'src/types/v2/dataLoaders'

interface Props {
  setPrecision: (precision: WritePrecision) => void
  precision: WritePrecision
}

const writePrecisions: WritePrecision[] = [
  WritePrecision.Ms,
  WritePrecision.S,
  WritePrecision.U,
  WritePrecision.Us,
  WritePrecision.Ns,
]

const transformPrecision = {
  [WritePrecision.Ns]: Precision.Nanoseconds,
  [WritePrecision.Us]: Precision.Microseconds,
  [WritePrecision.U]: Precision.U,
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
              {transformPrecision[value]}
            </Dropdown.Item>
          ))}
        </Dropdown>
      </div>
    )
  }
}

export default PrecisionDropdown
