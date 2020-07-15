// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {get, isEmpty} from 'lodash'

// Selectors
import {getSaveableView} from 'src/timeMachine/selectors'
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'
import {sortDashboardByName} from 'src/dashboards/selectors'

// Components
import {Form, Input, Button, Grid} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import DashboardsDropdown from 'src/dataExplorer/components/DashboardsDropdown'

import {
  DashboardTemplate,
  DEFAULT_DASHBOARD_NAME,
  DEFAULT_CELL_NAME,
} from 'src/dashboards/constants'

// Actions
import {getDashboards} from 'src/dashboards/actions/thunks'
import {
  createCellWithView,
  createDashboardWithView,
} from 'src/cells/actions/thunks'
import {notify} from 'src/shared/actions/notifications'

// Types
import {AppState, Dashboard, View, ResourceType} from 'src/types'
import {
  Columns,
  InputType,
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface State {
  targetDashboardIDs: string[]
  cellName: string
  isNameDashVisible: boolean
  newDashboardName: string
}

interface OwnProps {
  dismiss: () => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

@ErrorHandling
class SaveAsCellForm extends PureComponent<Props, State> {
  public state: State = {
    targetDashboardIDs: [],
    cellName: '',
    isNameDashVisible: false,
    newDashboardName: DEFAULT_DASHBOARD_NAME,
  }

  public componentDidMount() {
    const {onGetDashboards} = this.props
    onGetDashboards()
  }

  public render() {
    const {dismiss, dashboards} = this.props
    const {
      cellName,
      isNameDashVisible,
      targetDashboardIDs,
      newDashboardName,
    } = this.state
    return (
      <Form onSubmit={this.handleSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Target Dashboard(s)">
                <DashboardsDropdown
                  onSelect={this.handleSelectDashboardID}
                  selectedIDs={targetDashboardIDs}
                  dashboards={dashboards}
                  newDashboardName={newDashboardName}
                />
              </Form.Element>
            </Grid.Column>
            {isNameDashVisible && this.nameDashboard}
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Cell Name">
                <Input
                  type={InputType.Text}
                  placeholder="Add optional cell name"
                  name="cellName"
                  value={cellName}
                  onChange={this.handleChangeCellName}
                  testID="save-as-dashboard-cell--cell-name"
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Footer>
                <Button
                  text="Cancel"
                  onClick={dismiss}
                  titleText="Cancel save"
                  type={ButtonType.Button}
                  testID="save-as-dashboard-cell--cancel"
                />
                <Button
                  text="Save as Dashboard Cell"
                  testID="save-as-dashboard-cell--submit"
                  color={ComponentColor.Success}
                  type={ButtonType.Submit}
                  onClick={this.handleSubmit}
                  status={
                    this.isFormValid
                      ? ComponentStatus.Default
                      : ComponentStatus.Disabled
                  }
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get nameDashboard(): JSX.Element {
    const {newDashboardName} = this.state
    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Element label="New Dashboard Name">
          <Input
            type={InputType.Text}
            placeholder="Add dashboard name"
            name="dashboardName"
            value={newDashboardName}
            onChange={this.handleChangeDashboardName}
            testID="save-as-dashboard-cell--dashboard-name"
          />
        </Form.Element>
      </Grid.Column>
    )
  }

  private get isFormValid(): boolean {
    const {targetDashboardIDs} = this.state
    return !isEmpty(targetDashboardIDs)
  }

  private handleSubmit = () => {
    const {
      onCreateCellWithView,
      onCreateDashboardWithView,
      dashboards,
      view,
      dismiss,
      orgID,
    } = this.props
    const {targetDashboardIDs} = this.state

    const cellName = this.state.cellName || DEFAULT_CELL_NAME
    const newDashboardName =
      this.state.newDashboardName || DEFAULT_DASHBOARD_NAME

    const viewWithProps: View = {...view, name: cellName}

    try {
      targetDashboardIDs.forEach(dashID => {
        if (dashID === DashboardTemplate.id) {
          onCreateDashboardWithView(orgID, newDashboardName, viewWithProps)
          return
        }

        const selectedDashboard = dashboards.find(d => d.id === dashID)
        onCreateCellWithView(selectedDashboard.id, viewWithProps)
      })
    } catch (error) {
      console.error(error)
    } finally {
      this.resetForm()
      dismiss()
    }
  }

  private resetForm() {
    this.setState({
      targetDashboardIDs: [],
      cellName: '',
      isNameDashVisible: false,
      newDashboardName: DEFAULT_DASHBOARD_NAME,
    })
  }

  private handleSelectDashboardID = (
    selectedIDs: string[],
    value: Dashboard
  ) => {
    if (value.id === DashboardTemplate.id) {
      this.setState({
        isNameDashVisible: selectedIDs.includes(DashboardTemplate.id),
      })
    }
    this.setState({targetDashboardIDs: selectedIDs})
  }

  private handleChangeDashboardName = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({newDashboardName: e.target.value})
  }

  private handleChangeCellName = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({cellName: e.target.value})
  }
}

const mstp = (state: AppState) => {
  const view = getSaveableView(state)
  const org = getOrg(state)
  const dashboards = getAll<Dashboard>(state, ResourceType.Dashboards)

  return {
    dashboards: sortDashboardByName(dashboards),
    view,
    orgID: get(org, 'id', ''),
  }
}

const mdtp = {
  onGetDashboards: getDashboards,
  onCreateCellWithView: createCellWithView,
  onCreateDashboardWithView: createDashboardWithView,
  notify,
}

const connector = connect(mstp, mdtp)

export default connector(SaveAsCellForm)
