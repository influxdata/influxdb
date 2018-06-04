import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {FluxTable} from 'src/types'
import VisHeaderTabs from 'src/data_explorer/components/VisHeaderTabs'
import TableSidebar from 'src/ifql/components/TableSidebar'
import TimeMachineTable from 'src/ifql/components/TimeMachineTable'
import FluxGraph from 'src/ifql/components/FluxGraph'
import NoResults from 'src/ifql/components/NoResults'

interface Props {
  data: FluxTable[]
}

enum VisType {
  Table = 'Table View',
  Line = 'Line Graph',
}

interface State {
  selectedResultID: string | null
  visType: VisType
}

@ErrorHandling
class TimeMachineVis extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      selectedResultID: this.initialResultID,
      visType: VisType.Line,
    }
  }

  public componentDidUpdate() {
    if (!this.selectedResult) {
      this.setState({selectedResultID: this.initialResultID})
    }
  }

  public render() {
    const {visType} = this.state

    return (
      <div className="time-machine-visualization">
        <div className="time-machine-visualization--settings">
          <VisHeaderTabs
            view={visType}
            views={[VisType.Table, VisType.Line]}
            currentView={visType}
            onToggleView={this.selectVisType}
          />
        </div>
        <div className="time-machine-visualization--visualization">
          {this.vis}
        </div>
      </div>
    )
  }

  private get vis(): JSX.Element {
    const {visType} = this.state
    const {data} = this.props
    if (visType === VisType.Line) {
      return <FluxGraph data={data} />
    }

    return this.table
  }

  private get table(): JSX.Element {
    return (
      <>
        {this.showSidebar && (
          <TableSidebar
            data={this.props.data}
            selectedResultID={this.state.selectedResultID}
            onSelectResult={this.handleSelectResult}
          />
        )}
        <div className="time-machine--vis">
          {this.shouldShowTable && (
            <TimeMachineTable table={this.selectedResult} />
          )}
          {!this.hasResults && <NoResults />}
        </div>
      </>
    )
  }

  private get initialResultID(): string {
    return _.get(this.props.data, '0.id', null)
  }

  private handleSelectResult = (selectedResultID: string): void => {
    this.setState({selectedResultID})
  }

  private selectVisType = (visType: VisType): void => {
    this.setState({visType})
  }

  private get showSidebar(): boolean {
    return this.props.data.length > 1
  }

  private get hasResults(): boolean {
    return !!this.props.data.length
  }

  private get shouldShowTable(): boolean {
    return !!this.props.data && !!this.selectedResult
  }

  private get selectedResult(): FluxTable {
    return this.props.data.find(d => d.id === this.state.selectedResultID)
  }
}

export default TimeMachineVis
