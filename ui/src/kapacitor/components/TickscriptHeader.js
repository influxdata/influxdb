import React, {PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import LogsToggle from 'src/kapacitor/components/LogsToggle'
import ConfirmButton from 'src/shared/components/ConfirmButton'

const TickscriptHeader = ({
  task: {id},
  onSave,
  onSaveAndExit,
  unsavedChanges,
  areLogsVisible,
  areLogsEnabled,
  isNewTickscript,
  onToggleLogsVisibility,
}) =>
  <div className="page-header full-width">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">TICKscript Editor</h1>
      </div>
      {areLogsEnabled &&
        <LogsToggle
          areLogsVisible={areLogsVisible}
          areLogsEnabled={areLogsEnabled}
          onToggleLogsVisibility={onToggleLogsVisibility}
        />}
      <div className="page-header__right">
        <SourceIndicator />
        {isNewTickscript
          ? <button
              className="btn btn-success btn-sm"
              title="ID your TICKscript to save"
              onClick={onSave}
              disabled={!id}
            >
              Save New TICKscript
            </button>
          : <button
              className="btn btn-success btn-sm"
              title="You have unsaved changes"
              onClick={onSave}
              disabled={!unsavedChanges}
            >
              Save Changes
            </button>}
        {unsavedChanges
          ? <ConfirmButton
              text="Exit"
              confirmText="This will discard unsaved changes"
              confirmAction={onSaveAndExit}
            />
          : <button
              className="btn btn-default btn-sm"
              title="Return to Alert Rules Page"
              onClick={onSaveAndExit}
            >
              Exit
            </button>}
      </div>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TickscriptHeader.propTypes = {
  isNewTickscript: bool,
  onSave: func,
  areLogsVisible: bool,
  areLogsEnabled: bool,
  onToggleLogsVisibility: func.isRequired,
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
