// Libraries
import React, {PureComponent} from 'react'
import {RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'
import DashboardImportOverlay from 'src/dashboards/components/DashboardImportOverlay'
import CreateFromTemplateOverlay from 'src/templates/components/createFromTemplateOverlay/CreateFromTemplateOverlay'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {extractDashboardLimits} from 'src/cloud/utils/limits'

// Actions
import {createDashboard as createDashboardAction} from 'src/dashboards/actions/thunks'
import {setDashboardSort} from 'src/dashboards/actions/creators'

// Types
import {AppState, ResourceType, DashboardSortParams} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import {ComponentStatus, Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {DashboardSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

interface DispatchProps {
  createDashboard: typeof createDashboardAction
  setDashboardSort: typeof setDashboardSort
}

interface StateProps {
  limitStatus: LimitStatus
  sortOptions: DashboardSortParams
}

type Props = ReduxProps & RouteComponentProps<{orgID: string}>

interface State {
  searchTerm: string
}

@ErrorHandling
class DashboardIndex extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {createDashboard, limitStatus, sortOptions} = this.props
    const {searchTerm} = this.state
    return (
      <>
        <Page
          testID="empty-dashboards-list"
          titleTag={pageTitleSuffixer(['Dashboards'])}
        >
          <Page.Header fullWidth={false}>
            <Page.Title title="Dashboards" />
            <CloudUpgradeButton />
          </Page.Header>
          <Page.ControlBar fullWidth={false}>
            <Page.ControlBarLeft>
              <SearchWidget
                placeholderText="Filter dashboards..."
                onSearch={this.handleFilterDashboards}
                searchTerm={searchTerm}
              />
              <ResourceSortDropdown
                resourceType={ResourceType.Dashboards}
                sortDirection={sortOptions.sortDirection}
                sortKey={sortOptions.sortKey}
                sortType={sortOptions.sortType}
                onSelect={this.handleSort}
              />
            </Page.ControlBarLeft>
            <Page.ControlBarRight>
              <AddResourceDropdown
                onSelectNew={createDashboard}
                onSelectImport={this.summonImportOverlay}
                onSelectTemplate={this.summonImportFromTemplateOverlay}
                resourceName="Dashboard"
                canImportFromTemplate={true}
                status={this.addResourceStatus}
              />
            </Page.ControlBarRight>
          </Page.ControlBar>
          <Page.Contents
            className="dashboards-index__page-contents"
            fullWidth={false}
            scrollable={true}
          >
            <GetAssetLimits>
              <AssetLimitAlert
                resourceName="dashboards"
                limitStatus={limitStatus}
              />
              <DashboardsIndexContents
                searchTerm={searchTerm}
                onFilterChange={this.handleFilterDashboards}
                sortDirection={sortOptions.sortDirection}
                sortType={sortOptions.sortType}
                sortKey={sortOptions.sortKey}
              />
            </GetAssetLimits>
          </Page.Contents>
        </Page>
        <Switch>
          <Route
            path="/orgs/:orgID/dashboards-list/import/template"
            component={CreateFromTemplateOverlay}
          />
          <Route
            path="/orgs/:orgID/dashboards-list/import"
            component={DashboardImportOverlay}
          />
        </Switch>
      </>
    )
  }

  private handleSort = (
    sortKey: DashboardSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.props.setDashboardSort({sortKey, sortDirection, sortType})
  }

  private handleFilterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props
    history.push(`/orgs/${orgID}/dashboards-list/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props
    history.push(`/orgs/${orgID}/dashboards-list/import/template`)
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}

const mstp = (state: AppState) => {
  const {
    cloud: {limits},
  } = state
  const sortOptions = state.resources.dashboards['sortOptions']

  return {
    limitStatus: extractDashboardLimits(limits),
    sortOptions,
  }
}

const mdtp = {
  createDashboard: createDashboardAction,
  setDashboardSort,
}

export default connector(DashboardIndex)
