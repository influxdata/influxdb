import React, {SFC, PureComponent} from 'react'

import SourceIndicator from 'src/shared/components/SourceIndicator'
import LogsToggle from 'src/kapacitor/components/LogsToggle'
import ConfirmButton from 'src/shared/components/ConfirmButton'

import {DBRP} from 'src/types/kapacitor'

interface Task {
  dbrps: DBRP[]
  id: string
}

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
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">TICKscript Editor</h1>
      </div>
      {areLogsEnabled && (
        <LogsToggle
          areLogsVisible={areLogsVisible}
          onToggleLogsVisibility={onToggleLogsVisibility}
        />
      )}
      <div className="page-header__right">
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

interface SaveProps {
  onSave: () => void
  task: Task
  isNewTickscript: boolean
  unsavedChanges: boolean
}

export class TickscriptSave extends PureComponent<SaveProps> {
  public render() {
    const {onSave} = this.props

    return (
      <button
        className="btn btn-success btn-sm"
        title="You have unsaved changes"
        onClick={onSave}
        disabled={this.isDisabled}
      >
        {this.textContent}
      </button>
    )
  }

  private get textContent(): string {
    if (this.props.isNewTickscript) {
      return 'Save New TICKscript'
    }

    return 'Save Changes'
  }

  private get isDisabled(): boolean {
    const {isNewTickscript, unsavedChanges, task} = this.props
    const {id, dbrps} = task

    if (isNewTickscript) {
      return !id || !dbrps.length
    }

    return !unsavedChanges || !dbrps.length
  }
}

export default TickscriptHeader
