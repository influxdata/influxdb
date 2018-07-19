import React, {PureComponent} from 'react'
import {DBRP} from 'src/types/kapacitor'
import {ErrorHandling} from 'src/shared/decorators/errors'

export interface Task {
  dbrps: DBRP[]
  id: string
}

interface SaveProps {
  onSave: () => void
  task: Task
  isNewTickscript: boolean
  unsavedChanges: boolean
}

@ErrorHandling
class TickscriptSave extends PureComponent<SaveProps> {
  public render() {
    const {onSave} = this.props

    return (
      <button
        className="btn btn-success btn-sm"
        title={this.title}
        onClick={onSave}
        disabled={this.isDisabled}
      >
        {this.textContent}
      </button>
    )
  }

  private get title(): string {
    const {task} = this.props

    if (!task.id) {
      return 'Name your TICKscript to save'
    }

    if (!task.dbrps.length) {
      return 'Select databases to save'
    }
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

export default TickscriptSave
