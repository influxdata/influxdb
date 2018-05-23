import React, {PureComponent, CSSProperties} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScriptResult, ScriptStatus} from 'src/types'
import TableSidebar from 'src/ifql/components/TableSidebar'
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
          <FancyScrollbar />
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
}

export default TimeMachineVis
