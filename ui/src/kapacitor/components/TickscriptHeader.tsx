import React, {PureComponent} from 'react'

import PageHeader from 'src/shared/components/PageHeader'
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

class TickscriptHeader extends PureComponent<Props> {
  public render() {
    return (
      <PageHeader
        title="TICKscript Editor"
        fullWidth={true}
        sourceIndicator={true}
        renderCenter={this.logsToggle}
        renderOptions={this.saveAndExit}
      />
    )
  }

  private logsToggle = (): JSX.Element => {
    const {areLogsEnabled, areLogsVisible, onToggleLogsVisibility} = this.props

    return (
      <LogsToggle
        areLogsEnabled={areLogsEnabled}
        areLogsVisible={areLogsVisible}
        onToggleLogsVisibility={onToggleLogsVisibility}
      />
    )
  }

  private saveAndExit = (): JSX.Element => {
    const {task, onSave, onExit, unsavedChanges, isNewTickscript} = this.props

    if (unsavedChanges) {
      return (
        <>
          <TickscriptSave
            task={task}
            onSave={onSave}
            unsavedChanges={unsavedChanges}
            isNewTickscript={isNewTickscript}
          />
          <ConfirmButton
            text="Exit"
            confirmText="Discard unsaved changes?"
            confirmAction={onExit}
          />
        </>
      )
    }
    return (
      <>
        <TickscriptSave
          task={task}
          onSave={onSave}
          unsavedChanges={unsavedChanges}
          isNewTickscript={isNewTickscript}
        />
        <button
          className="btn btn-default btn-sm"
          title="Return to Alert Rules"
          onClick={onExit}
        >
          Exit
        </button>
      </>
    )
  }
}

export default TickscriptHeader
