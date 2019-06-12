// Libraries
import React, {FunctionComponent} from 'react'

interface Props {
  label: string
  checked: boolean
  onSetChecked: (checked: boolean) => void
}

// TODO: Replace this with the Clockface checkbox once available
//
// See https://github.com/influxdata/influxdb/issues/14125.
const Checkbox: FunctionComponent<Props> = ({label, checked, onSetChecked}) => {
  return (
    <label className={`fancy-checkbox ${checked ? 'checked' : ''}`}>
      <input
        type="checkbox"
        checked={!!checked}
        onChange={() => onSetChecked(!checked)}
      />
      {label}
    </label>
  )
}

export default Checkbox
