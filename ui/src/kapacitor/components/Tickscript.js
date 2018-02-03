import React, {PropTypes} from 'react'

import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'
import TickscriptEditorControls from 'src/kapacitor/components/TickscriptEditorControls'
import TickscriptEditorConsole from 'src/kapacitor/components/TickscriptEditorConsole'
import LogsTable from 'src/kapacitor/components/LogsTable'

const Tickscript = ({
  onSave,
  onExit,
  task,
  logs,
  consoleMessage,
  onSelectDbrps,
  onChangeScript,
  onChangeType,
  onChangeID,
  unsavedChanges,
  isNewTickscript,
  areLogsVisible,
  areLogsEnabled,
  onToggleExpandLog,
  onToggleLogsVisibility,
}) =>
  <div className="page">
    <TickscriptHeader
      task={task}
      onSave={onSave}
      onExit={onExit}
      unsavedChanges={unsavedChanges}
      areLogsVisible={areLogsVisible}
      areLogsEnabled={areLogsEnabled}
      onToggleLogsVisibility={onToggleLogsVisibility}
      isNewTickscript={isNewTickscript}
    />
    <div className="page-contents--split">
      <div className="tickscript">
        <TickscriptEditorControls
          isNewTickscript={isNewTickscript}
          onSelectDbrps={onSelectDbrps}
          onChangeType={onChangeType}
          onChangeID={onChangeID}
          task={task}
        />
        <TickscriptEditor
          script={task.tickscript}
          onChangeScript={onChangeScript}
        />
        <TickscriptEditorConsole
          consoleMessage={consoleMessage}
          unsavedChanges={unsavedChanges}
        />
      </div>
      {areLogsVisible
        ? <LogsTable logs={logs} onToggleExpandLog={onToggleExpandLog} />
        : null}
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

Tickscript.propTypes = {
  logs: arrayOf(shape()).isRequired,
  onSave: func.isRequired,
  onExit: func.isRequired,
  source: shape({
    id: string,
  }),
  areLogsVisible: bool,
  areLogsEnabled: bool,
  onToggleLogsVisibility: func.isRequired,
  task: shape({
    id: string,
    script: string,
    dbsrps: arrayOf(shape()),
  }).isRequired,
  onChangeScript: func.isRequired,
  onSelectDbrps: func.isRequired,
  consoleMessage: string,
  onChangeType: func.isRequired,
  onChangeID: func.isRequired,
  isNewTickscript: bool.isRequired,
  unsavedChanges: bool,
  onToggleExpandLog: func.isRequired,
}

export default Tickscript
