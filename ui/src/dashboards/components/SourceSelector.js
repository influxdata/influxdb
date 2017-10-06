import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

const SourceSelector = ({sources = [], selected, onSetQuerySource, queries}) =>
  sources.length > 1 && queries.length
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
    : <div className="source-selector" />

const {array, arrayOf, func, shape, string} = PropTypes

SourceSelector.propTypes = {
  sources: arrayOf(shape()).isRequired,
  onSetQuerySource: func.isRequired,
  selected: string,
  queries: array,
}

export default SourceSelector
