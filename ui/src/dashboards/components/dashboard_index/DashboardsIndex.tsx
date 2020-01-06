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
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import GetResources from 'src/shared/components/GetResources'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {extractDashboardLimits} from 'src/cloud/utils/limits'

// Actions
import {
  deleteDashboardAsync,
  updateDashboardAsync,
  createDashboard as createDashboardAction,
  cloneDashboard as cloneDashboardAction,
} from 'src/dashboards/actions'

// Types
import {AppState, ResourceType} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import {ComponentStatus} from '@influxdata/clockface'

interface DispatchProps {
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  createDashboard: typeof createDashboardAction
  cloneDashboard: typeof cloneDashboardAction
}

interface StateProps {
  limitStatus: LimitStatus
}

interface OwnProps {
  router: InjectedRouter
  params: {orgID: string}
}

type Props = DispatchProps & StateProps & OwnProps

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
    const {
      createDashboard,
      cloneDashboard,
      handleUpdateDashboard,
      handleDeleteDashboard,
      limitStatus,
    } = this.props
    const {searchTerm} = this.state
    return (
      <>
        <Page
          testID="empty-dashboards-list"
          titleTag={pageTitleSuffixer(['Dashboards'])}
        >
          <Page.Header fullWidth={false}>
            <Page.HeaderLeft>
              <PageTitleWithOrg title="Dashboards" />
            </Page.HeaderLeft>
            <Page.HeaderRight>
              <AddResourceDropdown
                onSelectNew={createDashboard}
                onSelectImport={this.summonImportOverlay}
                onSelectTemplate={this.summonImportFromTemplateOverlay}
                resourceName="Dashboard"
                canImportFromTemplate={true}
                status={this.addResourceStatus}
              />
            </Page.HeaderRight>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <GetResources
              resources={[ResourceType.Dashboards, ResourceType.Labels]}
            >
              <GetAssetLimits>
                <AssetLimitAlert
                  resourceName="dashboards"
                  limitStatus={limitStatus}
                />
                <DashboardsIndexContents
                  filterComponent={
                    <SearchWidget
                      placeholderText="Filter dashboards..."
                      onSearch={this.handleFilterDashboards}
                      searchTerm={searchTerm}
                    />
                  }
                  onDeleteDashboard={handleDeleteDashboard}
                  onCreateDashboard={createDashboard}
                  onCloneDashboard={cloneDashboard}
                  onUpdateDashboard={handleUpdateDashboard}
                  searchTerm={searchTerm}
                  onFilterChange={this.handleFilterDashboards}
                  onImportDashboard={this.summonImportOverlay}
                />
              </GetAssetLimits>
            </GetResources>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
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
  handleDeleteDashboard: deleteDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
  createDashboard: createDashboardAction,
  cloneDashboard: cloneDashboardAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
