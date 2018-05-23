import React, {PureComponent, CSSProperties} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScriptResult, ScriptStatus} from 'src/types'
import TableSidebar from 'src/ifql/components/TableSidebar'
import TimeMachineTable from 'src/ifql/components/TimeMachineTable'
import {HANDLE_PIXELS} from 'src/shared/constants'

interface Props {
  data: ScriptResult[]
  status: ScriptStatus
}

interface State {
  selectedResultID: string | null
}

@ErrorHandling
class TimeMachineVis extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {selectedResultID: null}
  }

  public render() {
    return (
      <div className="time-machine-visualization" style={this.style}>
        <TableSidebar
          data={this.props.data}
          selectedResultID={this.state.selectedResultID}
          onSelectResult={this.handleSelectResult}
        />
        <div className="time-machine--vis">
          {this.shouldShowTable && (
            <TimeMachineTable {...this.selectedResult} />
          )}
        </div>
      </div>
    )
  }

  private handleSelectResult = (selectedResultID: string): void => {
    this.setState({selectedResultID})
  }

  private get style(): CSSProperties {
    return {
      padding: `${HANDLE_PIXELS}px`,
    }
  }

  private get shouldShowTable(): boolean {
    return !!this.selectedResult
  }

  private get selectedResult(): ScriptResult {
    return this.props.data.find(d => d.id === this.state.selectedResultID)
  }
}

export default TimeMachineVis
