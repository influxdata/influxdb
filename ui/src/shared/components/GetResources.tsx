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
import {AppState, RemoteDataState, ResourceType} from 'src/types'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Selectors
import {getResourcesStatus} from 'src/shared/selectors/getResourcesStatus'

interface StateProps {
  remoteDataState: RemoteDataState
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

  private getResourceDetails(resource: ResourceType) {
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
  const remoteDataState = getResourcesStatus(state, resources)

  return {
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
