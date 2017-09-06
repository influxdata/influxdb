import React, {PropTypes} from 'react'
import {PERIODS} from 'src/kapacitor/constants'
import Dropdown from 'shared/components/Dropdown'

const periods = PERIODS.map(text => {
  return {text}
})

const Deadman = ({rule, onChange}) =>
  <div className="rule-section--row">
    <p>Send Alert if Data is missing for</p>
    <Dropdown
      className="dropdown-80"
      menuClass="dropdown-malachite"
      items={periods}
      selected={rule.values.period}
      onChoose={onChange}
    />
  </div>

const {shape, string, func} = PropTypes

Deadman.propTypes = {
  rule: shape({
    values: shape({
      period: string,
    }),
  }),
  onChange: func.isRequired,
}

export default Deadman
