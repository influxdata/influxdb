import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScriptResult} from 'src/types'
import TableSidebar from 'src/ifql/components/TableSidebar'
import TimeMachineTable from 'src/ifql/components/TimeMachineTable'
import {HANDLE_PIXELS} from 'src/shared/constants'
import NoResults from 'src/ifql/components/NoResults'

interface Props {
  data: ScriptResult[]
}

interface State {
  selectedResultID: string | null
}

@ErrorHandling
class TimeMachineVis extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {selectedResultID: this.initialResultID}
  }

  public componentDidUpdate(__, prevState) {
    if (prevState.selectedResultID === null) {
      this.setState({selectedResultID: this.initialResultID})
    }
  }

  public render() {
    return (
      <div className="time-machine-visualization" style={this.style}>
        {this.hasResults && (
          <TableSidebar
            data={this.props.data}
            selectedResultID={this.state.selectedResultID}
            onSelectResult={this.handleSelectResult}
          />
        )}
        <div className="time-machine--vis">
          {this.shouldShowTable && (
            <TimeMachineTable {...this.selectedResult} />
          )}
          {!this.hasResults && <NoResults />}
        </div>
      </div>
    )
  }

  private get initialResultID(): string {
    return _.get(this.props.data, '0.id', null)
  }

  private handleSelectResult = (selectedResultID: string): void => {
    this.setState({selectedResultID})
  }

  private get style(): CSSProperties {
    return {
      padding: `${HANDLE_PIXELS}px`,
    }
  }

  private get hasResults(): boolean {
    return !!this.props.data.length
  }

  private get shouldShowTable(): boolean {
    return !!this.props.data && !!this.selectedResult
  }

  private get selectedResult(): ScriptResult {
    return this.props.data.find(d => d.id === this.state.selectedResultID)
  }
}

export default TimeMachineVis
