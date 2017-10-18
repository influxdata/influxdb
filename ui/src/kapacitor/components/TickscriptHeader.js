import React, {PropTypes} from 'react'
import SourceIndicator from 'shared/components/SourceIndicator'
import LogsToggle from 'src/kapacitor/components/LogsToggle'

const TickscriptHeader = ({
  task: {id},
  onSave,
  areLogsVisible,
  isNewTickscript,
  onToggleLogsVisbility,
}) =>
  <div className="page-header full-width">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">TICKscript Editor</h1>
      </div>
      <LogsToggle
        areLogsVisible={areLogsVisible}
        onToggleLogsVisbility={onToggleLogsVisbility}
      />
      <div className="page-header__right">
        <SourceIndicator />
        <button
          className="btn btn-success btn-sm"
          title={id ? '' : 'ID your TICKscript to save'}
          onClick={onSave}
          disabled={!id}
        >
          {isNewTickscript ? 'Save New TICKscript' : 'Save TICKscript'}
        </button>
      </div>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TickscriptHeader.propTypes = {
  isNewTickscript: bool,
  onSave: func,
  areLogsVisible: bool,
  onToggleLogsVisbility: func.isRequired,
  task: shape({
    dbrps: arrayOf(
      shape({
        db: string,
        rp: string,
      })
    ),
  }),
}

export default TickscriptHeader
