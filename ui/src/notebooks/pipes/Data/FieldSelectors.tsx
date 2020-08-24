// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

// Utils
import {event} from 'src/cloud/utils/reporting'

type Props = {
  fields: string[]
}

const FieldSelectors: FC<Props> = ({fields}) => {
  const {data, update} = useContext(PipeContext)
  const selectedField = data?.field
  const updateFieldSelection = useCallback(
    (field: string): void => {
      let updated = field
      if (updated === selectedField) {
        event('Deselecting Field in Flow Query Builder')
        updated = ''
      } else {
        event('Selecting Field in Flow Query Builder', {field})
      }
      update({field: updated})
    },
    [update]
  )

  return (
    <>
      {fields.map(field => (
        <List.Item
          key={field}
          value={field}
          onClick={() => updateFieldSelection(field)}
          selected={field === selectedField}
          title={field}
          gradient={Gradients.GundamPilot}
          wrapText={true}
        >
          <List.Indicator type="dot" />
          <div className="selectors--item-value selectors--item__field">
            {field}
          </div>
          <div className="selectors--item-name">field</div>
          <div className="selectors--item-type">string</div>
        </List.Item>
      ))}
    </>
  )
}

export default FieldSelectors
