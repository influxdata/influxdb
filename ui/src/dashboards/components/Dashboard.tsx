// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import Cells from 'src/shared/components/cells/Cells'
import DashboardEmpty from 'src/dashboards/components/dashboard_empty/DashboardEmpty'
import {
  Page,
  SpinnerContainer,
  TechnoSpinner,
  RemoteDataState,
} from '@influxdata/clockface'

// Types
import {Cell, AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Utils
import {getCells} from 'src/cells/selectors'

interface StateProps {
  cells: Cell[]
  status: RemoteDataState
}
interface OwnProps {
  manualRefresh: number
}

type Props = OwnProps & StateProps

@ErrorHandling
class DashboardComponent extends PureComponent<Props> {
  public render() {
    const {cells, status, manualRefresh} = this.props

    return (
      <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
        <Page.Contents fullWidth={true} scrollable={true} className="dashboard">
          {!!cells.length ? (
            <Cells manualRefresh={manualRefresh} />
          ) : (
            <DashboardEmpty />
          )}
          {/* This element is used as a portal container for note tooltips in cell headers */}
          <div className="cell-header-note-tooltip-container" />
        </Page.Contents>
      </SpinnerContainer>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    cells: getCells(state, state.currentDashboard.id),
    status: state.resources.cells.status,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp, null)(DashboardComponent)
