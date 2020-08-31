// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Types
import {AppState} from 'src/types'

// Components
import {
  Panel,
  FlexBox,
  ComponentSize,
  FlexDirection,
  AlignItems,
  Label,
} from '@influxdata/clockface'
import CommunityTemplateListItem from 'src/templates/components/CommunityTemplateListItem'
import CommunityTemplateListGroup from 'src/templates/components/CommunityTemplateListGroup'

import {CommunityTemplateParameters} from 'src/templates/components/CommunityTemplateParameters'

import {toggleTemplateResourceInstall} from 'src/templates/actions/creators'
import {getResourceInstallCount} from 'src/templates/selectors'

import {event} from 'src/cloud/utils/reporting'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class CommunityTemplateResourceContentUnconnected extends PureComponent<Props> {
  render() {
    const {summary} = this.props
    if (!Object.keys(summary).length) {
      return (
        <Panel>
          <Panel.Header>Calculating template resource needs...</Panel.Header>
        </Panel>
      )
    }

    return (
      <FlexBox
        margin={ComponentSize.Small}
        direction={FlexDirection.Column}
        alignItems={AlignItems.Stretch}
        className="community-templates-installer"
      >
        <CommunityTemplateListGroup
          title="Dashboards"
          count={getResourceInstallCount(summary.dashboards)}
        >
          {Array.isArray(summary.dashboards) &&
            summary.dashboards.map(dashboard => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={dashboard.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={dashboard.shouldInstall}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'dashboards',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'dashboards',
                          dashboard.templateMetaName,
                          !dashboard.shouldInstall
                        )
                      }}
                      title={dashboard.name}
                      description={dashboard.description}
                    >
                      Charts: {dashboard.charts.length}
                    </CommunityTemplateListItem>
                  </FlexBox.Child>

                  {resourceHasEnvRefs(dashboard) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={dashboard} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Telegraf Configurations"
          count={getResourceInstallCount(summary.telegrafConfigs)}
        >
          {Array.isArray(summary.telegrafConfigs) &&
            summary.telegrafConfigs.map(telegrafConfig => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={telegrafConfig.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={telegrafConfig.shouldInstall}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'telegraf',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'telegrafConfigs',
                          telegrafConfig.templateMetaName,
                          !telegrafConfig.shouldInstall
                        )
                      }}
                      title={telegrafConfig.templateMetaName}
                      description={telegrafConfig.description}
                    />
                  </FlexBox.Child>
                  {resourceHasEnvRefs(telegrafConfig) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={telegrafConfig} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Buckets"
          count={getResourceInstallCount(summary.buckets)}
        >
          {Array.isArray(summary.buckets) &&
            summary.buckets.map(bucket => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={bucket.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldDisableToggle={true}
                      shouldInstall={true}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'buckets',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'buckets',
                          bucket.templateMetaName,
                          !bucket.shouldInstall
                        )
                      }}
                      key={bucket.templateMetaName}
                      title={bucket.name}
                      description={bucket.description}
                    />
                  </FlexBox.Child>
                  {resourceHasEnvRefs(bucket) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={bucket} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>

        <CommunityTemplateListGroup
          title="Tasks"
          count={getResourceInstallCount(summary.summaryTask)}
        >
          {Array.isArray(summary.summaryTask) &&
            summary.summaryTask.map(task => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={task.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={true}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'tasks',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'tasks',
                          task.templateMetaName,
                          !task.shouldInstall
                        )
                      }}
                      key={task.templateMetaName}
                      title={task.name}
                      description={task.description}
                    />
                  </FlexBox.Child>
                  {resourceHasEnvRefs(task) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={task} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>

        <CommunityTemplateListGroup
          title="Checks"
          count={getResourceInstallCount(summary.checks)}
        >
          {Array.isArray(summary.checks) &&
            summary.checks.map(check => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={check.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={check.shouldInstall}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'checks',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'checks',
                          check.templateMetaName,
                          !check.shouldInstall
                        )
                      }}
                      key={check.templateMetaName}
                      title={check.check.name}
                      description={check.description}
                    />
                  </FlexBox.Child>
                  {resourceHasEnvRefs(check) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={check} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Variables"
          count={getResourceInstallCount(summary.variables)}
        >
          {Array.isArray(summary.variables) &&
            summary.variables.map(variable => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={variable.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldDisableToggle={true}
                      shouldInstall={true}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'variables',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'variables',
                          variable.templateMetaName,
                          !variable.shouldInstall
                        )
                      }}
                      key={variable.templateMetaName}
                      title={variable.name}
                      description={variable.description}
                    >
                      Type: {variable.arguments.type}
                    </CommunityTemplateListItem>
                  </FlexBox.Child>
                  {resourceHasEnvRefs(variable) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={variable} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Notification Rules"
          count={getResourceInstallCount(summary.notificationRules)}
        >
          {Array.isArray(summary.notificationRules) &&
            summary.notificationRules.map(notificationRule => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={notificationRule.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={notificationRule.shouldInstall}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'notification rules',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'notificationRules',
                          notificationRule.templateMetaName,
                          !notificationRule.shouldInstall
                        )
                      }}
                      key={notificationRule.templateMetaName}
                      title={notificationRule.name}
                      description={notificationRule.description}
                    />
                  </FlexBox.Child>
                  {resourceHasEnvRefs(summary) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={summary} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Labels"
          count={getResourceInstallCount(summary.labels)}
        >
          {Array.isArray(summary.labels) &&
            summary.labels.map(label => {
              return (
                <FlexBox
                  margin={ComponentSize.Small}
                  direction={FlexDirection.Row}
                  alignItems={AlignItems.Stretch}
                  key={label.templateMetaName}
                >
                  <FlexBox.Child grow={1}>
                    <CommunityTemplateListItem
                      shouldInstall={label.shouldInstall}
                      handleToggle={() => {
                        event('template_resource_uncheck', {
                          templateResourceType: 'labels',
                        })
                        this.props.toggleTemplateResourceInstall(
                          'labels',
                          label.templateMetaName,
                          !label.shouldInstall
                        )
                      }}
                      key={label.templateMetaName}
                    >
                      <Label
                        description={label.properties.description}
                        name={label.name}
                        id={label.name}
                        color={label.properties.color}
                      />
                    </CommunityTemplateListItem>
                  </FlexBox.Child>
                  {resourceHasEnvRefs(label) && (
                    <FlexBox.Child>
                      <CommunityTemplateParameters resource={label} />
                    </FlexBox.Child>
                  )}
                </FlexBox>
              )
            })}
        </CommunityTemplateListGroup>
      </FlexBox>
    )
  }
}

const resourceHasEnvRefs = (resource: any): boolean => {
  return (
    resource.envReferences && Object.keys(resource.envReferences).length > 0
  )
}

const mstp = (state: AppState) => {
  return {summary: state.resources.templates.stagedCommunityTemplate.summary}
}

const mdtp = {
  toggleTemplateResourceInstall,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateResourceContent = connector(
  CommunityTemplateResourceContentUnconnected
)
