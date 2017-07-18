import React, {PropTypes} from 'react'
const STREAM = 'stream'
const BATCH = 'batch'

const TickscriptType = ({type, onChangeType}) => (
  <ul className="nav nav-tablist nav-tablist-sm">
    <li
      className={type === STREAM ? 'active' : ''}
      onClick={onChangeType(STREAM)}
    >
      Stream
    </li>
    <li
      className={type === BATCH ? 'active' : ''}
      onClick={onChangeType(BATCH)}
    >
      Batch
    </li>
  </ul>
)

const {string, func} = PropTypes

TickscriptType.propTypes = {
  type: string,
  onChangeType: func,
}

export default TickscriptType
