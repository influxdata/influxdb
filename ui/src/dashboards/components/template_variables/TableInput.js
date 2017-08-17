import React, {PropTypes} from 'react'

const TableInput = ({
  name,
  defaultValue,
  isEditing,
  onStartEdit,
  autoFocusTarget,
}) => {
  return isEditing
    ? <div name={name} style={{width: '100%'}}>
        <input
          required={true}
          name={name}
          autoFocus={name === autoFocusTarget}
          className="form-control input-sm tvm-input-edit"
          type="text"
          defaultValue={
            name === 'tempVar'
              ? defaultValue.replace(/\u003a/g, '') // remove ':'s
              : defaultValue
          }
        />
      </div>
    : <div style={{width: '100%'}} onClick={onStartEdit(name)}>
        <div className="tvm-input">
          {defaultValue}
        </div>
      </div>
}

const {bool, func, string} = PropTypes

TableInput.propTypes = {
  defaultValue: string,
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  name: string.isRequired,
  autoFocusTarget: string,
}

export default TableInput
