import React, {PureComponent} from 'react'
import GraphOptionsSortableField from 'src/dashboards/components/GraphOptionsSortableField'

// import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import _ from 'lodash'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
  displayOrder: number
}

interface Props {
  fields: Field[]
  onFieldUpdate: (field: Field) => void
}
interface State {
  fields: Field[]
}

class GraphOptionsCustomizeFields extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      fields: this.props.fields || [],
    }
    this.moveField = this.moveField.bind(this)
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.fields == this.props.fields) {
      return
    }
    this.setState({fields: nextProps.fields})
  }
  moveField(dragIndex, hoverIndex) {
    const {fields} = this.state
    const dragField = fields[dragIndex]
    const removedFields = _.concat(
      _.slice(fields, 0, dragIndex),
      _.slice(fields, dragIndex + 1)
    )
    const addedFields = _.concat(
      _.slice(removedFields, 0, hoverIndex),
      [dragField],
      _.slice(removedFields, hoverIndex)
    )
    this.setState({fields: addedFields})
  }

  public render() {
    const {fields} = this.state
    const {onFieldUpdate} = this.props
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
              moveField={this.moveField}
              onFieldUpdate={onFieldUpdate}
            />
          ))}
        </div>
      </div>
    )
  }
}

export default DragDropContext(HTML5Backend)(GraphOptionsCustomizeFields)
