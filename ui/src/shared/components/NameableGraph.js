import React, {PropTypes} from 'react'

const NameableGraph = ({
  cell: {
    name,
    isEditing,
    x,
    y,
  },
  cell,
  children,
  onEditCell,
  onRenameCell,
  onUpdateCell,
}) => {
  let nameOrField
  const isEditable = !!(onEditCell || onRenameCell || onUpdateCell)

  if (isEditing && isEditable) {
    nameOrField = (
      <input
        type="text"
        value={name}
        autoFocus={true}
        onChange={onRenameCell(x, y)}
        onBlur={onUpdateCell(cell)}
        onKeyUp={(evt) => {
          if (evt.key === 'Enter') {
            onUpdateCell(cell)()
          }
        }}
      />
    )
  } else {
    nameOrField = name
  }

  let onClickHandler
  if (isEditable) {
    onClickHandler = onEditCell
  } else {
    onClickHandler = () => {
      // no-op
    }
  }

  return (
    <div>
      <h2 className="dash-graph--heading" onClick={onClickHandler(x, y, isEditing)}>{nameOrField}</h2>
      <div className="dash-graph--container">
        {children}
      </div>
    </div>
  )
}

const {
  func,
  node,
  number,
  shape,
  string,
} = PropTypes

NameableGraph.propTypes = {
  cell: shape({
    name: string.isRequired,
    x: number.isRequired,
    y: number.isRequired,
  }).isRequired,
  children: node.isRequired,
  onEditCell: func,
  onRenameCell: func,
  onUpdateCell: func,
}

export default NameableGraph;
