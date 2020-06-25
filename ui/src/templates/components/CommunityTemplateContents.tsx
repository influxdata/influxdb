// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {AppState, CommunityTemplate} from 'src/types'

import {Heading, HeadingElement, Page, Panel} from '@influxdata/clockface'

interface StateProps {
  activeCommunityTemplate: CommunityTemplate
}

class CommunityTemplateContentsUnconnected extends PureComponent<StateProps> {
  render() {
    const {activeCommunityTemplate} = this.props
    if (!Object.keys(activeCommunityTemplate).length) {
      return (
        <Page>
          <Panel>
            <Panel.Header>Calculating template resource needs...</Panel.Header>
          </Panel>
        </Page>
      )
    }

    return (
      <Page className="community-templates-installer">
        {Array.isArray(activeCommunityTemplate.dashboards) &&
          activeCommunityTemplate.dashboards.map(dashboard => {
            return (
              <Panel key={dashboard.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>Dashboard</Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {dashboard.name}
                  <br />
                  {dashboard.description}
                  <br />
                  Charts: {dashboard.charts.length}
                </Panel.Body>
              </Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.telegrafConfigs) &&
          activeCommunityTemplate.telegrafConfigs.map(telegrafConfig => {
            return (
              <Panel.Panel key={telegrafConfig.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>
                    Telegraf Configuration
                  </Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {telegrafConfig.pkgName}
                  <br />
                  {telegrafConfig.description}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.buckets) &&
          activeCommunityTemplate.buckets.map(bucket => {
            return (
              <Panel.Panel key={bucket.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>Bucket</Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {bucket.name}
                  <br />
                  {bucket.description}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.checks) &&
          activeCommunityTemplate.checks.map(check => {
            return (
              <Panel.Panel key={check.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>Check</Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {check.check.name}
                  <br />
                  {check.description}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.variables) &&
          activeCommunityTemplate.variables.map(variable => {
            return (
              <Panel.Panel key={variable.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>Variable</Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {variable.name}
                  <br />
                  Type: {variable.arguments.type}
                  <br />
                  {variable.description}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.notificationRules) &&
          activeCommunityTemplate.notificationRules.map(notificationRule => {
            return (
              <Panel.Panel key={notificationRule.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>
                    Notification Rule
                  </Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {notificationRule.name}
                  <br />
                  {notificationRule.description}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
        {Array.isArray(activeCommunityTemplate.labels) &&
          activeCommunityTemplate.labels.map(label => {
            return (
              <Panel.Panel key={label.pkgName}>
                <Panel.Header>
                  <Heading element={HeadingElement.H4}>Label</Heading>
                </Panel.Header>
                <Panel.Body>
                  Name: {label.name}
                  <br />
                  {Object.keys(label.properties).map(property => {
                    return (
                      <aside key={property}>
                        {property}: {label.properties[property]}
                      </aside>
                    )
                  })}
                </Panel.Body>
              </Panel.Panel>
            )
          })}
      </Page>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeCommunityTemplate} = state.resources.templates

  return {activeCommunityTemplate}
}

export const CommunityTemplateContents = connect<StateProps, {}, {}>(
  mstp,
  null
)(CommunityTemplateContentsUnconnected)
