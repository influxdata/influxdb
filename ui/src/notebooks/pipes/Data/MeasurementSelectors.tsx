// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

// Utils
import {event} from 'src/cloud/utils/reporting'

type Props = {
  measurements: string[]
}

const MeasurementSelectors: FC<Props> = ({measurements}) => {
  const {data, update} = useContext(PipeContext)
  const selectedMeasurement = data?.measurement
  const updateMeasurementSelection = useCallback(
    (measurement: string): void => {
      let updated = measurement
      if (updated === selectedMeasurement) {
        event('Deselecting Measurement in Flow Query Builder')
        updated = ''
      } else {
        event('Selecting Measurement in Flow Query Builder', {measurement})
      }

      update({measurement: updated})
    },
    [update]
  )
  return (
    <>
      {measurements.map(measurement => (
        <List.Item
          key={measurement}
          value={measurement}
          onClick={() => updateMeasurementSelection(measurement)}
          selected={measurement === selectedMeasurement}
          title={measurement}
          gradient={Gradients.GundamPilot}
          wrapText={true}
        >
          <List.Indicator type="dot" />
          <div className="selectors--item-value selectors--item__measurement">
            {measurement}
          </div>
          <div className="selectors--item-name">measurement</div>
          <div className="selectors--item-type">string</div>
        </List.Item>
      ))}
    </>
  )
}

export default MeasurementSelectors
