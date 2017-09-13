import React, {PropTypes} from 'react'

import Dropdown from 'src/shared/components/Dropdown'

const SourceSelector = ({sources = []}) =>
  <div className="display-options--cell">
    <h5 className="display-options--header">Source Selector</h5>
    <div className="form-group col-sm-12">
      <Dropdown
        onChoose={() => {}}
        items={sources}
        selected={sources[0].text}
      />
    </div>
  </div>

const {arrayOf, shape} = PropTypes

SourceSelector.propTypes = {
  sources: arrayOf(shape()).isRequired,
}

export default SourceSelector
