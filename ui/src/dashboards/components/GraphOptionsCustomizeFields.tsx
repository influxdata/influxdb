import React, {SFC} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
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
      <FancyScrollbar
        className="customize-fields"
        maxHeight={225}
        autoHeight={true}
      >
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
      </FancyScrollbar>
    </div>
  )
}

export default GraphOptionsCustomizeFields
