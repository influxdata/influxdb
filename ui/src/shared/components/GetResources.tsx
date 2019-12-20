// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Actions
import {getLabels} from 'src/labels/actions'
import {getBuckets} from 'src/buckets/actions'
import {getTelegrafs} from 'src/telegrafs/actions'
import {getPlugins} from 'src/dataLoaders/actions/telegrafEditor'
import {getVariables} from 'src/variables/actions'
import {getScrapers} from 'src/scrapers/actions'
import {getDashboardsAsync} from 'src/dashboards/actions'
import {getTasks} from 'src/tasks/actions'
import {getAuthorizations} from 'src/authorizations/actions'
import {getTemplates} from 'src/templates/actions'
import {getMembers} from 'src/members/actions'
import {getChecks} from 'src/alerting/actions/checks'
import {getNotificationRules} from 'src/alerting/actions/notifications/rules'
import {getEndpoints} from 'src/alerting/actions/notifications/endpoints'

// Types
import {AppState, RemoteDataState} from 'src/types'
import {LabelsState} from 'src/labels/reducers'
import {BucketsState} from 'src/buckets/reducers'
import {TelegrafsState} from 'src/telegrafs/reducers'
import {PluginResourceState} from 'src/dataLoaders/reducers/telegrafEditor'
import {ScrapersState} from 'src/scrapers/reducers'
import {TasksState} from 'src/tasks/reducers/'
import {DashboardsState} from 'src/dashboards/reducers/dashboards'
import {AuthorizationsState} from 'src/authorizations/reducers'
import {VariablesState} from 'src/variables/reducers'
import {TemplatesState} from 'src/templates/reducers'
import {MembersState} from 'src/members/reducers'
import {ChecksState} from 'src/alerting/reducers/checks'
import {NotificationRulesState} from 'src/alerting/reducers/notifications/rules'
import {NotificationEndpointsState} from 'src/alerting/reducers/notifications/endpoints'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Selectors
import {getResourcesStatus} from 'src/shared/selectors/getResourcesStatus'

interface StateProps {
  remoteDataState: RemoteDataState
  labels: LabelsState
  buckets: BucketsState
  telegrafs: TelegrafsState
  plugins: PluginResourceState
  variables: VariablesState
  scrapers: ScrapersState
  tokens: AuthorizationsState
  dashboards: DashboardsState
  templates: TemplatesState
  tasks: TasksState
  members: MembersState
  checks: ChecksState
  rules: NotificationRulesState
  endpoints: NotificationEndpointsState
}

interface DispatchProps {
  getLabels: typeof getLabels
  getBuckets: typeof getBuckets
  getTelegrafs: typeof getTelegrafs
  getPlugins: typeof getPlugins
  getVariables: typeof getVariables
  getScrapers: typeof getScrapers
  getAuthorizations: typeof getAuthorizations
  getDashboards: typeof getDashboardsAsync
  getTasks: typeof getTasks
  getTemplates: typeof getTemplates
  getMembers: typeof getMembers
  getChecks: typeof getChecks
  getNotificationRules: typeof getNotificationRules
  getEndpoints: typeof getEndpoints
}

interface PassedProps {
  resources: Array<ResourceType>
}

export type Props = StateProps & DispatchProps & PassedProps

export enum ResourceType {
  Labels = 'labels',
  Buckets = 'buckets',
  Telegrafs = 'telegrafs',
  Plugins = 'plugins',
  Variables = 'variables',
  Authorizations = 'tokens',
  Scrapers = 'scrapers',
  Dashboards = 'dashboards',
  Tasks = 'tasks',
  Templates = 'templates',
  Members = 'members',
  Checks = 'checks',
  NotificationRules = 'rules',
  NotificationEndpoints = 'endpoints',
}

@ErrorHandling
class GetResources extends PureComponent<Props, StateProps> {
  public componentDidMount() {
    const {resources} = this.props
    const promises = []
    resources.forEach(resource => {
      promises.push(this.getResourceDetails(resource))
    })
    Promise.all(promises)
  }

  private getResourceDetails(resource) {
    switch (resource) {
      case ResourceType.Dashboards: {
        return this.props.getDashboards()
      }

      case ResourceType.Labels: {
        return this.props.getLabels()
      }

      case ResourceType.Buckets: {
        return this.props.getBuckets()
      }

      case ResourceType.Telegrafs: {
        return this.props.getTelegrafs()
      }

      case ResourceType.Plugins: {
        return this.props.getPlugins()
      }

      case ResourceType.Scrapers: {
        return this.props.getScrapers()
      }

      case ResourceType.Variables: {
        return this.props.getVariables()
      }

      case ResourceType.Tasks: {
        return this.props.getTasks()
      }

      case ResourceType.Authorizations: {
        return this.props.getAuthorizations()
      }

      case ResourceType.Templates: {
        return this.props.getTemplates()
      }

      case ResourceType.Members: {
        return this.props.getMembers()
      }

      case ResourceType.Checks: {
        return this.props.getChecks()
      }

      case ResourceType.NotificationRules: {
        return this.props.getNotificationRules()
      }

      case ResourceType.NotificationEndpoints: {
        return this.props.getEndpoints()
      }

      default: {
        throw new Error('incorrect resource type provided')
      }
    }
  }

  public render() {
    const {children, remoteDataState} = this.props

    return (
      <SpinnerContainer
        loading={remoteDataState}
        spinnerComponent={<TechnoSpinner />}
      >
        {children}
      </SpinnerContainer>
    )
  }
}

const mstp = (state: AppState, {resources}: Props): StateProps => {
  const {
    labels,
    buckets,
    telegrafs,
    plugins,
    variables,
    scrapers,
    tokens,
    dashboards,
    tasks,
    templates,
    members,
    checks,
    rules,
    endpoints,
  } = state

  const remoteDataState = getResourcesStatus(state, resources)

  return {
    labels,
    buckets,
    telegrafs,
    plugins,
    dashboards,
    variables,
    scrapers,
    tokens,
    tasks,
    templates,
    members,
    checks,
    rules,
    endpoints,
    remoteDataState,
  }
}

const mdtp = {
  getLabels: getLabels,
  getBuckets: getBuckets,
  getTelegrafs: getTelegrafs,
  getPlugins: getPlugins,
  getVariables: getVariables,
  getScrapers: getScrapers,
  getAuthorizations: getAuthorizations,
  getDashboards: getDashboardsAsync,
  getTasks: getTasks,
  getTemplates: getTemplates,
  getMembers: getMembers,
  getChecks: getChecks,
  getNotificationRules: getNotificationRules,
  getEndpoints: getEndpoints,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetResources)
