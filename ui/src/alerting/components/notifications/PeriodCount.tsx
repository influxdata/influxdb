// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, InputType} from '@influxdata/clockface'

interface Props {
  period: string
  count: number
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const PeriodCount: FC<Props> = ({period, count, onChange}) => {
  return (
    <div className="period-count--container">
      <Input
        className="count-input"
        type={InputType.Number}
        name="count"
        value={count}
        placeholder="1"
        onChange={onChange}
      />
      <div className="sentence-frag">instances in the last</div>
      <Input
        className="period-input"
        name="period"
        value={period}
        placeholder="1h"
        onChange={onChange}
      />
    </div>
  )
}

export default PeriodCount
