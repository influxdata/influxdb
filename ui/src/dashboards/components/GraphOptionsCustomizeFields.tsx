import React, {SFC} from 'react'
import GraphOptionsSortableField from 'src/dashboards/components/GraphOptionsSortableField'

// import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
  order?: number
}

interface Props {
  fields: Field[]
  onFieldUpdate: (field: Field) => void
  moveField: (dragIndex: number, hoverIndex: number) => void
}
const GraphOptionsCustomizeFields: SFC<Props> = ({
  fields,
  onFieldUpdate,
  moveField,
}) => {
  return (
    <div className="graph-options-group">
      <label className="form-label">Customize Fields</label>
      <div>
        {fields.map((field, i) => (
          <GraphOptionsSortableField
            key={field.internalName}
            index={i}
            id={field.internalName}
            internalName={field.internalName}
            displayName={field.displayName}
            onFieldUpdate={onFieldUpdate}
            moveField={moveField}
          />
        ))}
      </div>
    </div>
  )
}

export default DragDropContext(HTML5Backend)(GraphOptionsCustomizeFields)
