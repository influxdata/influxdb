// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/timeMachine/components/ToolbarTab.scss'

interface Props {
  onSetActive: () => void
  name: string
  active: boolean
}

export default class ToolbarTab extends PureComponent<Props> {
  public render() {
    const {active, onSetActive, name} = this.props
    return (
      <div
        className={`toolbar-tab ${active ? 'active' : ''}`}
        onClick={onSetActive}
        title={name}
      >
        {name}
      </div>
    )
  }
}
