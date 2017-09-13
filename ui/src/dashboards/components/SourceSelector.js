import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

const SourceSelector = ({sources = [], selected, onSetCellSource}) =>
  <div className="display-options--cell">
    <h5 className="display-options--header">InfluxDB Source Selector</h5>
    <div
      className="form-group col-md-12"
      style={{display: 'flex', flexDirection: 'column'}}
    >
      <label>Using data from</label>
      <Dropdown
        items={sources}
        buttonSize="btn-sm"
        menuClass="dropdown-astronaut"
        useAutoComplete={true}
        selected={selected}
        onChoose={onSetCellSource}
        toggleStyle={{width: '50%', maxWidth: '300px'}}
      />
    </div>
  </div>

const {arrayOf, func, shape, string} = PropTypes

SourceSelector.propTypes = {
  sources: arrayOf(shape()).isRequired,
  onSetCellSource: func.isRequired,
  selected: string,
}

export default SourceSelector
