import React, {PropTypes} from 'react'
import classnames from 'classnames'

import CustomTimeIndicator from 'shared/components/CustomTimeIndicator'

const NameableGraphHeader = ({
  onCancelEditCell,
  isEditable,
  onRenameCell,
  onUpdateCell,
  cell,
  cellName,
  cell: {i, name, queries},
}) => {
  const isInputVisible = isEditable && cell.isEditing
  const className = classnames('dash-graph--heading', {
    'dash-graph--heading-draggable': isEditable,
  })
  const onKeyUp = evt => {
    if (evt.key === 'Enter') {
      onUpdateCell({...cell, name: cellName})()
    }

    if (evt.key === 'Escape') {
      onCancelEditCell(i)
    }
  }

  return (
    <div className={className}>
      {isInputVisible
        ? <GraphNameInput
            value={cellName}
            onChange={onRenameCell}
            onBlur={onUpdateCell({...cell, name: cellName})}
            onKeyUp={onKeyUp}
          />
        : <GraphName name={name} queries={queries} />}
    </div>
  )
}

const {arrayOf, bool, func, string, shape} = PropTypes

NameableGraphHeader.propTypes = {
  cell: shape(),
  cellName: string,
  onRenameCell: func,
  onUpdateCell: func,
  isEditable: bool,
  onCancelEditCell: func,
}

const GraphName = ({name, queries}) =>
  <span className="dash-graph--name">
    {name}
    {queries && queries.length
      ? <CustomTimeIndicator queries={queries} />
      : null}
  </span>

GraphName.propTypes = {
  name: string,
  queries: arrayOf(shape()),
}

const GraphNameInput = ({value, onKeyUp, onChange, onBlur}) =>
  <input
    className="form-control input-sm dash-graph--name-edit"
    type="text"
    value={value}
    autoFocus={true}
    onChange={onChange}
    onBlur={onBlur}
    onKeyUp={onKeyUp}
  />

GraphNameInput.propTypes = {
  value: string,
  onKeyUp: func,
  onChange: func,
  onBlur: func,
}

export default NameableGraphHeader
