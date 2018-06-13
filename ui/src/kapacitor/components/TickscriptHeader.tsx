import React, {SFC} from 'react'

import SourceIndicator from 'src/shared/components/SourceIndicator'
import LogsToggle from 'src/kapacitor/components/LogsToggle'
import ConfirmButton from 'src/shared/components/ConfirmButton'
import TickscriptSave, {Task} from 'src/kapacitor/components/TickscriptSave'

interface Props {
  task: Task
  unsavedChanges: boolean
  areLogsVisible: boolean
  areLogsEnabled: boolean
  isNewTickscript: boolean
  onSave: () => void
  onExit: () => void
  onToggleLogsVisibility: () => void
}

const TickscriptHeader: SFC<Props> = ({
  task,
  onSave,
  onExit,
  unsavedChanges,
  areLogsEnabled,
  areLogsVisible,
  isNewTickscript,
  onToggleLogsVisibility,
}) => (
  <div className="page-header full-width">
    <div className="page-header--container">
      <div className="page-header--left">
        <h1 className="page-header--title">TICKscript Editor</h1>
      </div>
      {areLogsEnabled && (
        <LogsToggle
          areLogsVisible={areLogsVisible}
          onToggleLogsVisibility={onToggleLogsVisibility}
        />
      )}
      <div className="page-header--right">
        <SourceIndicator />
        <TickscriptSave
          task={task}
          onSave={onSave}
          unsavedChanges={unsavedChanges}
          isNewTickscript={isNewTickscript}
        />
        {unsavedChanges ? (
          <ConfirmButton
            text="Exit"
            confirmText="Discard unsaved changes?"
            confirmAction={onExit}
          />
        ) : (
          <button
            className="btn btn-default btn-sm"
            title="Return to Alert Rules"
            onClick={onExit}
          >
            Exit
          </button>
        )}
      </div>
    </div>
  </div>
)

export default TickscriptHeader
