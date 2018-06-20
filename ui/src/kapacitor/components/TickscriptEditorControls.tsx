import React, {Component, MouseEvent, ChangeEvent} from 'react'

import TickscriptType from 'src/kapacitor/components/TickscriptType'
import MultiSelectDBDropdown from 'src/shared/components/MultiSelectDBDropdown'
import TickscriptID, {
  TickscriptStaticID,
} from 'src/kapacitor/components/TickscriptID'

import {Task} from 'src/types'
import {DBRP} from 'src/types/kapacitor'

interface DBRPDropdownItem extends DBRP {
  name: string
}

interface Props {
  isNewTickscript: boolean
  onSelectDbrps: (dbrps: DBRP[]) => void
  onChangeType: (type: string) => (event: MouseEvent<HTMLLIElement>) => void
  onChangeID: (e: ChangeEvent<HTMLInputElement>) => void
  task: Task
}

class TickscriptEditorControls extends Component<Props> {
  public render() {
    const {onSelectDbrps, onChangeType, task} = this.props
    return (
      <div className="tickscript-controls">
        {this.tickscriptID}
        <div className="tickscript-controls--right">
          <TickscriptType type={task.type} onChangeType={onChangeType} />
          <MultiSelectDBDropdown
            selectedItems={this.addName(task.dbrps)}
            onApply={onSelectDbrps}
          />
        </div>
      </div>
    )
  }

  private get tickscriptID() {
    const {isNewTickscript, onChangeID, task} = this.props

    if (isNewTickscript) {
      return <TickscriptID onChangeID={onChangeID} id={task.id} />
    }

    return <TickscriptStaticID id={this.taskID} />
  }

  private get taskID() {
    const {
      task: {name, id},
    } = this.props
    if (name) {
      return name
    }
    return id
  }

  private addName = (list: DBRP[]): DBRPDropdownItem[] => {
    const listWithName = list.map(l => ({...l, name: `${l.db}.${l.rp}`}))
    return listWithName
  }
}

export default TickscriptEditorControls
