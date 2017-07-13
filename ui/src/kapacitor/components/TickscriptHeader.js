import React, {PropTypes} from 'react'
import SourceIndicator from 'shared/components/SourceIndicator'

const TickscriptHeader = ({source, onSave}) => (
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title kapacitor-theme">
          TICKscript Editor
        </h1>
      </div>
      <div className="page-header__right">
        <SourceIndicator sourceName={source.name} />
        <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>
      </div>
    </div>
  </div>
)

const {func, shape} = PropTypes

TickscriptHeader.propTypes = {
  onSave: func,
  source: shape(),
}

export default TickscriptHeader
