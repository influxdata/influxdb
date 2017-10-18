import React, {PropTypes} from 'react'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'
import TickscriptEditorControls from 'src/kapacitor/components/TickscriptEditorControls'
import TickscriptEditorConsole from 'src/kapacitor/components/TickscriptEditorConsole'
import LogsTable from 'src/kapacitor/components/LogsTable'

const Tickscript = ({
  onSave,
  task,
  validation,
  onSelectDbrps,
  onChangeScript,
  onChangeType,
  onChangeID,
  isNewTickscript,
  areLogsVisible,
  onToggleLogsVisbility,
}) =>
  <div className="page">
    <TickscriptHeader
      task={task}
      onSave={onSave}
      areLogsVisible={areLogsVisible}
      onToggleLogsVisbility={onToggleLogsVisbility}
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
        <TickscriptEditorConsole validation={validation} />
        <TickscriptEditor
          script={task.tickscript}
          onChangeScript={onChangeScript}
        />
      </div>
      {areLogsVisible ? <LogsTable /> : null}
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

Tickscript.propTypes = {
  onSave: func.isRequired,
  areLogsVisible: bool,
  onToggleLogsVisbility: func.isRequired,
  task: shape({
    id: string,
    script: string,
    dbsrps: arrayOf(shape()),
  }).isRequired,
  onChangeScript: func.isRequired,
  onSelectDbrps: func.isRequired,
  validation: string,
  onChangeType: func.isRequired,
  onChangeID: func.isRequired,
  isNewTickscript: bool.isRequired,
}

export default Tickscript
