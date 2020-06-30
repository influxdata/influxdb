// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState, CommunityTemplate} from 'src/types'

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

import {toggleTemplateResourceInstall} from 'src/templates/actions/creators'
import {getResourceInstallCount} from 'src/templates/selectors'

interface StateProps {
  activeCommunityTemplate: CommunityTemplate
}

interface DispatchProps {
  toggleTemplateResourceInstall: typeof toggleTemplateResourceInstall
}

type Props = StateProps & DispatchProps

class CommunityTemplateContentsUnconnected extends PureComponent<Props> {
  render() {
    const {activeCommunityTemplate} = this.props
    if (!Object.keys(activeCommunityTemplate).length) {
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
          count={getResourceInstallCount(activeCommunityTemplate.dashboards)}
        >
          {Array.isArray(activeCommunityTemplate.dashboards) &&
            activeCommunityTemplate.dashboards.map(dashboard => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={dashboard.shouldInstall}
                  handleToggle={() => {
                    this.props.toggleTemplateResourceInstall(
                      'dashboards',
                      dashboard.templateMetaName,
                      !dashboard.shouldInstall
                    )
                  }}
                  key={dashboard.templateMetaName}
                  title={dashboard.name}
                  description={dashboard.description}
                >
                  Charts: {dashboard.charts.length}
                </CommunityTemplateListItem>
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Telegraf Configurations"
          count={getResourceInstallCount(
            activeCommunityTemplate.telegrafConfigs
          )}
        >
          {Array.isArray(activeCommunityTemplate.telegrafConfigs) &&
            activeCommunityTemplate.telegrafConfigs.map(telegrafConfig => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={telegrafConfig.shouldInstall}
                  handleToggle={() => {
                    this.props.toggleTemplateResourceInstall(
                      'telegrafConfigs',
                      telegrafConfig.templateMetaName,
                      !telegrafConfig.shouldInstall
                    )
                  }}
                  key={telegrafConfig.templateMetaName}
                  title={telegrafConfig.templateMetaName}
                  description={telegrafConfig.description}
                />
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Buckets"
          count={getResourceInstallCount(activeCommunityTemplate.buckets)}
        >
          {Array.isArray(activeCommunityTemplate.buckets) &&
            activeCommunityTemplate.buckets.map(bucket => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={bucket.shouldInstall}
                  handleToggle={() => {
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
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Checks"
          count={getResourceInstallCount(activeCommunityTemplate.checks)}
        >
          {Array.isArray(activeCommunityTemplate.checks) &&
            activeCommunityTemplate.checks.map(check => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={check.shouldInstall}
                  handleToggle={() => {
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
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Variables"
          count={getResourceInstallCount(activeCommunityTemplate.variables)}
        >
          {Array.isArray(activeCommunityTemplate.variables) &&
            activeCommunityTemplate.variables.map(variable => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={variable.shouldInstall}
                  handleToggle={() => {
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
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Notification Rules"
          count={getResourceInstallCount(
            activeCommunityTemplate.notificationRules
          )}
        >
          {Array.isArray(activeCommunityTemplate.notificationRules) &&
            activeCommunityTemplate.notificationRules.map(notificationRule => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={notificationRule.shouldInstall}
                  handleToggle={() => {
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
              )
            })}
        </CommunityTemplateListGroup>
        <CommunityTemplateListGroup
          title="Labels"
          count={getResourceInstallCount(activeCommunityTemplate.labels)}
        >
          {Array.isArray(activeCommunityTemplate.labels) &&
            activeCommunityTemplate.labels.map(label => {
              return (
                <CommunityTemplateListItem
                  shouldInstall={label.shouldInstall}
                  handleToggle={() => {
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
              )
            })}
        </CommunityTemplateListGroup>
      </FlexBox>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeCommunityTemplate} = state.resources.templates

  return {activeCommunityTemplate}
}

const mdtp = {
  toggleTemplateResourceInstall,
}

export const CommunityTemplateContents = connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(CommunityTemplateContentsUnconnected)
