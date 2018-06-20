import React, {PureComponent, MouseEvent, ChangeEvent} from 'react'

import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptEditor from 'src/kapacitor/components/TickscriptEditor'
import TickscriptEditorControls from 'src/kapacitor/components/TickscriptEditorControls'
import TickscriptEditorConsole from 'src/kapacitor/components/TickscriptEditorConsole'
import LogsTable from 'src/kapacitor/components/LogsTable'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {Task} from 'src/types'
import {LogItem, DBRP} from 'src/types/kapacitor'

interface Props {
  logs: LogItem[]
  onSave: () => void
  onExit: () => void
  areLogsVisible: boolean
  areLogsEnabled: boolean
  onToggleLogsVisibility: () => void
  task: Task
  onChangeScript: (tickscript: string) => void
  onSelectDbrps: (dbrps: DBRP[]) => void
  consoleMessage: string
  onChangeType: (type: string) => (event: MouseEvent<HTMLLIElement>) => void
  onChangeID: (e: ChangeEvent<HTMLInputElement>) => void
  isNewTickscript: boolean
  unsavedChanges: boolean
}

@ErrorHandling
class Tickscript extends PureComponent<Props> {
  public render() {
    const {
      onSave,
      onExit,
      task,
      consoleMessage,
      onSelectDbrps,
      onChangeScript,
      onChangeType,
      onChangeID,
      unsavedChanges,
      isNewTickscript,
      areLogsVisible,
      areLogsEnabled,
      onToggleLogsVisibility,
    } = this.props
    return (
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
          <div className="tickscript" style={this.style}>
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
          {this.logsTable}
        </div>
      </div>
    )
  }

  private get style() {
    const {areLogsVisible} = this.props
    if (areLogsVisible) {
      return {maxWidth: '50%'}
    }
  }

  private get logsTable() {
    const {areLogsVisible, logs} = this.props

    if (areLogsVisible) {
      return <LogsTable logs={logs} />
    }
  }
}

export default Tickscript
