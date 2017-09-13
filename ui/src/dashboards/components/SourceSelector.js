import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

const noop = () => {}

const SourceSelector = ({sources = [], source}) =>
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
        selected={sources.find(s => s.id === source.id).text || '(No values)'}
        onChoose={noop}
        toggleStyle={{width: '50%'}}
      />
    </div>
  </div>

const {arrayOf, shape} = PropTypes

SourceSelector.propTypes = {
  sources: arrayOf(shape()).isRequired,
  source: shape({}).isRequired,
}

export default SourceSelector
