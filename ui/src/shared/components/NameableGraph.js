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

  if (isEditing) {
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

  return (
    <div>
      <h2 className="dash-graph--heading" onClick={onEditCell(x, y, isEditing)}>{nameOrField}</h2>
      <div className="dash-graph--container">
        {children}
      </div>
    </div>
  )
}

const {
  func,
  node,
  shape,
  string,
} = PropTypes

NameableGraph.propTypes = {
  cell: shape({
    name: string.isRequired,
    x: string.isRequired,
    y: string.isRequired,
  }).isRequired,
  children: node.isRequired,
  onEditCell: func.isRequired,
  onRenameCell: func.isRequired,
  onUpdateCell: func.isRequired,
}

export default NameableGraph;
