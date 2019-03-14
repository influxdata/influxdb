// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Input, Tabs} from 'src/clockface'
import {IconFont} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// APIs
import {createDashboard, cloneDashboard} from 'src/dashboards/apis/'

// Actions
import {
  deleteDashboardAsync,
  updateDashboardAsync,
} from 'src/dashboards/actions/'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {dashboardCreateFailed} from 'src/shared/copy/notifications'
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'

// Types
import {Notification} from 'src/types/notifications'
import {Dashboard} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DispatchProps {
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  notify: (message: Notification) => void
}

interface OwnProps {
  dashboards: Dashboard[]
  onChange: () => void
  orgID: string
}

type Props = DispatchProps & OwnProps & WithRouterProps

interface State {
  searchTerm: string
}

@ErrorHandling
class Dashboards extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {dashboards, notify, handleUpdateDashboard} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter dashboards..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
            testID={`dashboards--filter-field ${searchTerm}`}
            customClass="filter-dashboards"
          />
          <AddResourceDropdown
            onSelectNew={this.handleCreateDashboard}
            onSelectImport={this.summonImportOverlay}
            resourceName="Dashboard"
          />
        </Tabs.TabContentsHeader>
        <DashboardsIndexContents
          dashboards={dashboards}
          onDeleteDashboard={this.handleDeleteDashboard}
          onCreateDashboard={this.handleCreateDashboard}
          onCloneDashboard={this.handleCloneDashboard}
          onUpdateDashboard={handleUpdateDashboard}
          notify={notify}
          searchTerm={searchTerm}
          showOwnerColumn={false}
          onFilterChange={this.handleFilterUpdate}
        />
      </>
    )
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {router, params} = this.props
    router.push(`/organizations/${params.orgID}/dashboards/import`)
  }

  private handleCreateDashboard = async (): Promise<void> => {
    const {router, notify, orgID} = this.props
    try {
      const newDashboard = {
        name: DEFAULT_DASHBOARD_NAME,
        cells: [],
        orgID: orgID,
      }
      const data = await createDashboard(newDashboard)
      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      notify(dashboardCreateFailed())
    }
  }

  private handleCloneDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const {router, notify, dashboards} = this.props
    try {
      const data = await cloneDashboard(dashboard, dashboards)
      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      console.error(error)
      notify(dashboardCreateFailed())
    }
  }

  private handleDeleteDashboard = (dashboard: Dashboard) => {
    this.props.handleDeleteDashboard(dashboard)
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  handleDeleteDashboard: deleteDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter(Dashboards))
