import React, {PureComponent} from 'react'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'

interface RenamableField {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  fields: RenamableField[]
  onFieldUpdate: (field: RenamableField) => void
  moveField: (dragIndex: number, hoverIndex: number) => void
}

class GraphOptionsCustomizeFields extends PureComponent<Props> {
  public render() {
    const {fields, onFieldUpdate, moveField} = this.props

    return (
      <div className="graph-options-group">
        <label className="form-label">Customize Fields</label>
        <div>
          {fields.map((field, i) => (
            <GraphOptionsCustomizableField
              key={field.internalName}
              index={i}
              id={field.internalName}
              internalName={field.internalName}
              displayName={field.displayName}
              visible={field.visible}
              onFieldUpdate={onFieldUpdate}
              moveField={moveField}
            />
          ))}
        </div>
      </div>
    )
  }
}

export default DragDropContext(HTML5Backend)(GraphOptionsCustomizeFields)
