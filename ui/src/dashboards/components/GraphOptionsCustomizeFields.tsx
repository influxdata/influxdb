import React, {PureComponent} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'
import uuid from 'uuid'
import {SortableContainer, SortableElement, arrayMove} from 'react-sortable-hoc'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  fields: Field[]
  onFieldUpdate: (field: Field) => void
}
interface State {
  items: Field[]
}

const SortableItem = SortableElement(({value, onFieldUpdate}) => (
  <li>
    <GraphOptionsCustomizableField
      internalName={value.internalName}
      displayName={value.displayName}
      visible={value.visible}
      onFieldUpdate={onFieldUpdate}
    />
  </li>
))

const SortableList = SortableContainer(({items, onFieldUpdate}) => {
  return (
    <ul>
      {items.map((value, index) => (
        <SortableItem
          key={`item-${index}`}
          index={index}
          value={value}
          onFieldUpdate={onFieldUpdate}
        />
      ))}
    </ul>
  )
})

export default class GraphOptionsCustomizeFields extends PureComponent<
  Props,
  State
> {
  constructor(props) {
    super(props)
    this.state = {
      items: this.props.fields,
    }
  }
  onSortEnd = ({oldIndex, newIndex}) => {
    this.setState({
      items: arrayMove(this.state.items, oldIndex, newIndex),
    })
  }
  componentWillReceiveProps(nextProps) {
    if (nextProps.fields == this.props.fields) {
      return
    }
    this.setState({items: nextProps.fields})
  }

  public render() {
    const {items} = this.state
    const {onFieldUpdate} = this.props
    console.log('items', items)
    return (
      <div className="graph-options-group">
        <label className="form-label">Customize Fields</label>
        <SortableList
          items={items}
          onSortEnd={this.onSortEnd}
          onFieldUpdate={onFieldUpdate}
        />
      </div>
    )
  }
}
// <FancyScrollbar
//   className="customize-fields"
//   maxHeight={225}
//   autoHeight={true}
// >
// </FancyScrollbar>
