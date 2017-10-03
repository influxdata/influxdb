import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

const SourceSelector = ({sources = [], selected, onSetQuerySource}) =>
  sources.length > 1
    ? <div className="source-selector">
        <h3>Source:</h3>
        <Dropdown
          items={sources}
          buttonSize="btn-sm"
          menuClass="dropdown-astronaut"
          useAutoComplete={true}
          selected={selected}
          onChoose={onSetQuerySource}
          className="dropdown-240"
        />
      </div>
    : null

const {arrayOf, func, shape, string} = PropTypes

SourceSelector.propTypes = {
  sources: arrayOf(shape()).isRequired,
  onSetQuerySource: func.isRequired,
  selected: string,
}

export default SourceSelector
