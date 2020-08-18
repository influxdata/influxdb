// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

type Props = {
  measurement: string
}

const MeasurementSelector: FC<Props> = ({measurement}) => {
  const {data, update} = useContext(PipeContext)
  const selectedMeasurement = data.measurement
  const updateMeasurementSelection = useCallback(
    (value: string): void => {
      let updated = value
      if (updated === selectedMeasurement) {
        updated = ''
      }

      update({measurement: updated})
    },
    [update]
  )
  return (
    <List.Item
      value={measurement}
      onClick={() => updateMeasurementSelection(measurement)}
      selected={measurement === selectedMeasurement}
      title={measurement}
      gradient={Gradients.GundamPilot}
      wrapText={true}
    >
      <List.Indicator type="dot" />
      <div className="data-measurement--equation">
        {`_measurement = ${measurement}`}
      </div>
      <div className="data-measurement--name">&nbsp;measurement&nbsp;</div>
      <div className="data-measurement--type">string</div>
    </List.Item>
  )
}

export default MeasurementSelector
