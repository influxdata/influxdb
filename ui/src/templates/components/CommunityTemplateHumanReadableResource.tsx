// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

// Selectors
import {getAll} from 'src/resources/selectors'

import {event} from 'src/cloud/utils/reporting'

// Types
import {
  AppState,
  Dashboard,
  Task,
  Label,
  Bucket,
  Telegraf,
  Variable,
  ResourceType,
  Check,
  NotificationEndpoint,
  NotificationRule,
} from 'src/types'

interface ComponentProps {
  link: string
  metaName: string
  resourceID: string
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = ComponentProps & ReduxProps

const recordClick = () => {
  event('template_view_resource')
}

const CommunityTemplateHumanReadableResourceUnconnected: FC<Props> = ({
  link,
  metaName,
  resourceID,
  allResources,
}) => {
  const matchingResource = allResources.find(
    resource => resource.id === resourceID
  )

  const humanName = matchingResource ? matchingResource.name : metaName

  return (
    <Link to={link} onClick={recordClick}>
      <code>{humanName}</code>
    </Link>
  )
}

const mstp = (state: AppState) => {
  const labels = getAll<Label>(state, ResourceType.Labels)
  const cleanedLabels = labels.map(label => ({
    id: label.id,
    name: label.name,
  }))

  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  const cleanedBuckets = buckets.map(bucket => ({
    id: bucket.id,
    name: bucket.name,
  }))

  const telegrafs = getAll<Telegraf>(state, ResourceType.Telegrafs)
  const cleanedTelegrafs = telegrafs.map(telegraf => ({
    id: telegraf.id,
    name: telegraf.name,
  }))

  const variables = getAll<Variable>(state, ResourceType.Variables)
  const cleanedVariables = variables.map(variable => ({
    id: variable.id,
    name: variable.name,
  }))

  const dashboards = getAll<Dashboard>(state, ResourceType.Dashboards)
  const cleanedDashboards = dashboards.map(dashboard => ({
    id: dashboard.id,
    name: dashboard.name,
  }))

  const tasks = getAll<Task>(state, ResourceType.Tasks)
  const cleanedTasks = tasks.map(task => ({
    id: task.id,
    name: task.name,
  }))

  const checks = getAll<Check>(state, ResourceType.Checks)
  const cleanedChecks = checks.map(check => ({
    id: check.id,
    name: check.name,
  }))

  const notificationEndpoints = getAll<NotificationEndpoint>(
    state,
    ResourceType.NotificationEndpoints
  )
  const cleanedNotificationEndpoints = notificationEndpoints.map(
    notificationEndpoint => ({
      id: notificationEndpoint.id,
      name: notificationEndpoint.name,
    })
  )

  const notificationRules = getAll<NotificationRule>(
    state,
    ResourceType.NotificationRules
  )
  const cleanedNotificationRules = notificationRules.map(notificationRule => {
    if (notificationRule.id && notificationRule.name) {
      return {
        id: notificationRule.id,
        name: notificationRule.name,
      }
    }
  })

  const allResources = [
    ...cleanedLabels,
    ...cleanedBuckets,
    ...cleanedTelegrafs,
    ...cleanedVariables,
    ...cleanedDashboards,
    ...cleanedTasks,
    ...cleanedChecks,
    ...cleanedNotificationEndpoints,
    ...cleanedNotificationRules,
  ]

  return {
    allResources,
  }
}

const connector = connect(mstp)

export const CommunityTemplateHumanReadableResource = connector(
  CommunityTemplateHumanReadableResourceUnconnected
)
