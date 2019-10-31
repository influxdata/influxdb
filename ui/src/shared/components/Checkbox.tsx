// Libraries
import React, {FC} from 'react'

interface Props {
  label: string
  checked: boolean
  onSetChecked: (checked: boolean) => void
  testID?: string
}

// TODO: Replace this with the Clockface checkbox once available
//
// See https://github.com/influxdata/influxdb/issues/14125.
const Checkbox: FC<Props> = ({label, checked, onSetChecked, testID}) => {
  return (
    <label className={`fancy-checkbox ${checked ? 'checked' : ''}`}>
      <input
        data-testid={testID || 'checkbox'}
        type="checkbox"
        checked={!!checked}
        onChange={() => onSetChecked(!checked)}
      />
      {label}
    </label>
  )
}

export default Checkbox
