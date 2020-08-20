// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

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
        updated = ''
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
          <div className="data-field--equation">{`_field = ${field}`}</div>
          <div className="data-measurement--name">&nbsp;field&nbsp;</div>
          <div className="data-measurement--type">string</div>
        </List.Item>
      ))}
    </>
  )
}

export default FieldSelectors
