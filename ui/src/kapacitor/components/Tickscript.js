import React, {PropTypes} from 'react'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'
import ResizeContainer from 'shared/components/ResizeContainer'
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
    {areLogsVisible
      ? <ResizeContainer containerClass="page-contents">
          <div>
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
          <LogsTable isWidget={false} />
        </ResizeContainer>
      : <div className="page-contents">
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
        </div>}
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
