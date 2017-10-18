import React, {PropTypes} from 'react'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'
import LogsTable from 'src/kapacitor/components/LogsTable'

const Tickscript = ({
  source,
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
      source={source}
      onSave={onSave}
      onChangeID={onChangeID}
      onChangeType={onChangeType}
      onSelectDbrps={onSelectDbrps}
      isNewTickscript={isNewTickscript}
      areLogsVisible={areLogsVisible}
      onToggleLogsVisbility={onToggleLogsVisbility}
    />
    <div className="tickscript-wrapper">
      <div className="tickscript">
        <div className="tickscript-controls">
          <h1 className="tickscript-name">sdfsdfsdf</h1>
          <div>CONTROLS</div>
        </div>
        <div className="tickscript-console">
          <div className="tickscript-console--output">
            {validation
              ? <p>
                  {validation}
                </p>
              : <p className="tickscript-console--default">
                  Save your TICKscript to validate it
                </p>}
          </div>
        </div>
        <div className="tickscript-editor">
          <TickscriptEditor
            script={task.tickscript}
            onChangeScript={onChangeScript}
          />
        </div>
      </div>
      {areLogsVisible ? <LogsTable /> : null}
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

Tickscript.propTypes = {
  onSave: func.isRequired,
  areLogsVisible: bool,
  onToggleLogsVisbility: func.isRequired,
  source: shape(),
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
