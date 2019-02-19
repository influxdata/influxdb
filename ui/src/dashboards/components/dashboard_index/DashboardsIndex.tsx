// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from 'src/pageLayout'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import {OverlayTechnology} from 'src/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import ImportOverlay from 'src/shared/components/ImportOverlay'
import ExportOverlay from 'src/shared/components/ExportOverlay'
import EditLabelsOverlay from 'src/shared/components/EditLabelsOverlay'

// Utils
import {getDeep} from 'src/utils/wrappers'

// APIs
import {createDashboard, cloneDashboard} from 'src/dashboards/apis/v2/'

// Actions
import {
  getDashboardsAsync,
  importDashboardAsync,
  deleteDashboardAsync,
  updateDashboardAsync,
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
} from 'src/dashboards/actions/v2'
import {setDefaultDashboard} from 'src/shared/actions/links'
import {retainRangesDashTimeV1 as retainRangesDashTimeV1Action} from 'src/dashboards/actions/v2/ranges'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {
  dashboardSetDefaultFailed,
  dashboardCreateFailed,
} from 'src/shared/copy/notifications'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'

// Types
import {Notification} from 'src/types/notifications'
import {Links, Cell, Dashboard, AppState, Organization} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DispatchProps {
  handleSetDefaultDashboard: typeof setDefaultDashboard
  handleGetDashboards: typeof getDashboardsAsync
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleImportDashboard: typeof importDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  notify: (message: Notification) => void
  retainRangesDashTimeV1: (dashboardIDs: string[]) => void
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
}

interface StateProps {
  links: Links
  dashboards: Dashboard[]
  orgs: Organization[]
}

interface OwnProps {
  router: InjectedRouter
}

type Props = DispatchProps & StateProps & OwnProps

interface State {
  searchTerm: string
  isImportingDashboard: boolean
  isExportingDashboard: boolean
  exportDashboard: Dashboard
  isEditingDashboardLabels: boolean
  dashboardLabelsEdit: Dashboard
}

@ErrorHandling
class DashboardIndex extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      isImportingDashboard: false,
      isExportingDashboard: false,
      exportDashboard: null,
      isEditingDashboardLabels: false,
      dashboardLabelsEdit: null,
    }
  }

  public async componentDidMount() {
    const {handleGetDashboards, dashboards} = this.props
    await handleGetDashboards()
    const dashboardIDs = dashboards.map(d => d.id)
    this.props.retainRangesDashTimeV1(dashboardIDs)
  }

  public render() {
    const {dashboards, notify, links, handleUpdateDashboard, orgs} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Page titleTag="Dashboards">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Dashboards" />
            </Page.Header.Left>
            <Page.Header.Right>
              <SearchWidget
                placeholderText="Filter dashboards by name..."
                onSearch={this.filterDashboards}
              />
              <AddResourceDropdown
                onSelectNew={this.handleCreateDashboard}
                onSelectImport={this.handleToggleImportOverlay}
                resourceName="Dashboard"
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-md-12">
              <DashboardsIndexContents
                dashboards={dashboards}
                orgs={orgs}
                onSetDefaultDashboard={this.handleSetDefaultDashboard}
                defaultDashboardLink={links.defaultDashboard}
                onDeleteDashboard={this.handleDeleteDashboard}
                onCreateDashboard={this.handleCreateDashboard}
                onCloneDashboard={this.handleCloneDashboard}
                onExportDashboard={this.handleExportDashboard}
                onUpdateDashboard={handleUpdateDashboard}
                onEditLabels={this.handleStartEditingLabels}
                notify={notify}
                searchTerm={searchTerm}
                showOwnerColumn={true}
              />
            </div>
          </Page.Contents>
        </Page>
        {this.importOverlay}
        {this.exportOverlay}
        {this.labelEditorOverlay}
      </>
    )
  }

  private handleSetDefaultDashboard = async (
    defaultDashboardLink: string
  ): Promise<void> => {
    const {dashboards, notify, handleSetDefaultDashboard} = this.props
    const {name} = dashboards.find(d => d.links.self === defaultDashboardLink)

    try {
      await handleSetDefaultDashboard(defaultDashboardLink)
    } catch (error) {
      console.error(error)
      notify(dashboardSetDefaultFailed(name))
    }
  }

  private handleCreateDashboard = async (): Promise<void> => {
    const {router, notify, orgs} = this.props
    try {
      const newDashboard = {
        name: DEFAULT_DASHBOARD_NAME,
        cells: [],
        orgID: orgs[0].id,
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
    const {router, notify, orgs, dashboards} = this.props
    try {
      const data = await cloneDashboard(
        {
          ...dashboard,
          orgID: orgs[0].id,
        },
        dashboards
      )
      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      console.error(error)
      notify(dashboardCreateFailed())
    }
  }

  private handleDeleteDashboard = (dashboard: Dashboard) => {
    this.props.handleDeleteDashboard(dashboard)
  }

  private handleExportDashboard = (dashboard: Dashboard): void => {
    this.setState({exportDashboard: dashboard, isExportingDashboard: true})
  }

  private handleImportDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const defaultCell = {
      x: 0,
      y: 0,
      w: 4,
      h: 4,
    }

    const name = get(dashboard, 'name', DEFAULT_DASHBOARD_NAME)
    const cellsWithDefaultsApplied = getDeep<Cell[]>(
      dashboard,
      'cells',
      []
    ).map(c => ({...defaultCell, ...c}))

    await this.props.handleImportDashboard({
      ...dashboard,
      name,
      cells: cellsWithDefaultsApplied,
    })
  }

  private filterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private handleToggleImportOverlay = (): void => {
    this.setState({isImportingDashboard: !this.state.isImportingDashboard})
  }

  private handleToggleExportOverlay = (): void => {
    this.setState({isExportingDashboard: !this.state.isExportingDashboard})
  }

  private get importOverlay(): JSX.Element {
    const {notify} = this.props
    const {isImportingDashboard} = this.state

    return (
      <ImportOverlay
        isVisible={isImportingDashboard}
        resourceName="Dashboard"
        onDismissOverlay={this.handleToggleImportOverlay}
        onImport={this.handleImportDashboard}
        notify={notify}
        isResourceValid={this.handleValidateDashboard}
      />
    )
  }

  private get exportOverlay(): JSX.Element {
    const {isExportingDashboard, exportDashboard} = this.state

    return (
      <ExportOverlay
        resource={exportDashboard}
        isVisible={isExportingDashboard}
        resourceName="Dashboard"
        onDismissOverlay={this.handleToggleExportOverlay}
      />
    )
  }

  private handleStartEditingLabels = (dashboardLabelsEdit: Dashboard): void => {
    this.setState({dashboardLabelsEdit, isEditingDashboardLabels: true})
  }

  private handleStopEditingLabels = (): void => {
    this.setState({isEditingDashboardLabels: false})
  }

  private handleValidateDashboard = (): boolean => {
    return true
  }

  private get labelEditorOverlay(): JSX.Element {
    const {onAddDashboardLabels, onRemoveDashboardLabels} = this.props
    const {isEditingDashboardLabels, dashboardLabelsEdit} = this.state

    return (
      <OverlayTechnology visible={isEditingDashboardLabels}>
        <EditLabelsOverlay<Dashboard>
          resource={dashboardLabelsEdit}
          onDismissOverlay={this.handleStopEditingLabels}
          onAddLabels={onAddDashboardLabels}
          onRemoveLabels={onRemoveDashboardLabels}
        />
      </OverlayTechnology>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {dashboards, links, orgs} = state

  return {
    orgs,
    dashboards,
    links,
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  handleSetDefaultDashboard: setDefaultDashboard,
  handleGetDashboards: getDashboardsAsync,
  handleDeleteDashboard: deleteDashboardAsync,
  handleImportDashboard: importDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
  retainRangesDashTimeV1: retainRangesDashTimeV1Action,
  onAddDashboardLabels: addDashboardLabelsAsync,
  onRemoveDashboardLabels: removeDashboardLabelsAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
