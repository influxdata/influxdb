// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import DashboardsSortDropdown from 'src/dashboards/components/dashboard_index/DashboardsSortDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {extractDashboardLimits} from 'src/cloud/utils/limits'

// Actions
import {createDashboard as createDashboardAction} from 'src/dashboards/actions/thunks'

// Types
import {AppState, Dashboard} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import {ComponentStatus, Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'

interface DispatchProps {
  createDashboard: typeof createDashboardAction
}

interface StateProps {
  limitStatus: LimitStatus
}

interface OwnProps {
  router: InjectedRouter
  params: {orgID: string}
}

type Props = DispatchProps & StateProps & OwnProps

export type SortKey = keyof Dashboard | 'meta.updatedAt'

interface State {
  searchTerm: string
  sortDirection: Sort
  sortType: SortTypes
  sortKey: SortKey
}

@ErrorHandling
class DashboardIndex extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
      sortKey: 'name',
    }
  }

  public render() {
    const {createDashboard, limitStatus} = this.props
    const {searchTerm, sortDirection, sortType, sortKey} = this.state
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
              <DashboardsSortDropdown
                sortDirection={sortDirection}
                sortKey={sortKey}
                sortType={sortType}
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
                sortDirection={sortDirection}
                sortType={sortType}
                sortKey={sortKey}
              />
            </GetAssetLimits>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleSort = (
    sortKey: SortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private handleFilterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import/template`)
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    cloud: {limits},
  } = state

  return {
    limitStatus: extractDashboardLimits(limits),
  }
}

const mdtp: DispatchProps = {
  createDashboard: createDashboardAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
