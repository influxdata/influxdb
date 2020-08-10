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

import {toggleTemplateResourceInstall} from 'src/templates/actions/creators'
import {getResourceInstallCount} from 'src/templates/selectors'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class CommunityTemplateOverlayContentsUnconnected extends PureComponent<Props> {
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
          count={getResourceInstallCount(summary.telegrafConfigs)}
        >
          {Array.isArray(summary.telegrafConfigs) &&
            summary.telegrafConfigs.map(telegrafConfig => {
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
          count={getResourceInstallCount(summary.buckets)}
        >
          {Array.isArray(summary.buckets) &&
            summary.buckets.map(bucket => {
              return (
                <CommunityTemplateListItem
                  shouldDisableToggle={true}
                  shouldInstall={true}
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
          count={getResourceInstallCount(summary.checks)}
        >
          {Array.isArray(summary.checks) &&
            summary.checks.map(check => {
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
          count={getResourceInstallCount(summary.variables)}
        >
          {Array.isArray(summary.variables) &&
            summary.variables.map(variable => {
              return (
                <CommunityTemplateListItem
                  shouldDisableToggle={true}
                  shouldInstall={true}
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
          count={getResourceInstallCount(summary.notificationRules)}
        >
          {Array.isArray(summary.notificationRules) &&
            summary.notificationRules.map(notificationRule => {
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
          count={getResourceInstallCount(summary.labels)}
        >
          {Array.isArray(summary.labels) &&
            summary.labels.map(label => {
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

const mstp = (state: AppState) => {
  return {summary: state.resources.templates.communityTemplateToInstall.summary}
}

const mdtp = {
  toggleTemplateResourceInstall,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateOverlayContents = connector(
  CommunityTemplateOverlayContentsUnconnected
)
