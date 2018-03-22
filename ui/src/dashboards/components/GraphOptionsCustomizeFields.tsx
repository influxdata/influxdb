import React, {SFC} from 'react'

import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'
import uuid from 'uuid'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  fields: Field[]
  onFieldUpdate: (field: Field) => void
}

const GraphOptionsCustomizeFields: SFC<Props> = ({fields, onFieldUpdate}) => {
  return (
    <div className="graph-options-group">
      <label className="form-label">Customize Fields</label>
      <div className="field-controls--group">
        {fields.map(field => {
          return (
            <GraphOptionsCustomizableField
              key={uuid.v4()}
              internalName={field.internalName}
              displayName={field.displayName}
              visible={field.visible}
              onFieldUpdate={onFieldUpdate}
            />
          )
        })}
      </div>
    </div>
  )
}

export default GraphOptionsCustomizeFields
