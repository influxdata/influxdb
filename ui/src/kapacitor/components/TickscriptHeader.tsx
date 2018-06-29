import React, {Component} from 'react'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
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

class TickscriptHeader extends Component<Props> {
  public render() {
    return (
      <PageHeader
        titleText="TICKscript Editor"
        fullWidth={true}
        sourceIndicator={true}
        optionsComponents={this.optionsComponents}
      />
    )
  }

  private get optionsComponents(): JSX.Element {
    const {
      task,
      onSave,
      unsavedChanges,
      isNewTickscript,
      areLogsEnabled,
      areLogsVisible,
      onToggleLogsVisibility,
    } = this.props

    return (
      <>
        <LogsToggle
          areLogsEnabled={areLogsEnabled}
          areLogsVisible={areLogsVisible}
          onToggleLogsVisibility={onToggleLogsVisibility}
        />
        <TickscriptSave
          task={task}
          onSave={onSave}
          unsavedChanges={unsavedChanges}
          isNewTickscript={isNewTickscript}
        />
        {this.saveButton}
      </>
    )
  }

  private get saveButton(): JSX.Element {
    const {unsavedChanges, onExit} = this.props

    if (unsavedChanges) {
      return (
        <ConfirmButton
          text="Exit"
          confirmText="Discard unsaved changes?"
          confirmAction={onExit}
        />
      )
    }

    return (
      <button
        className="btn btn-default btn-sm"
        title="Return to Alert Rules"
        onClick={onExit}
      >
        Exit
      </button>
    )
  }
}

export default TickscriptHeader
