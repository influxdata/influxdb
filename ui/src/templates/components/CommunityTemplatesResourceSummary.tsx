// Libraries
import React, {FC, useState} from 'react'
import classnames from 'classnames'

// Components
import {Icon, IconFont} from '@influxdata/clockface'
import {CommunityTemplateHumanReadableResource} from 'src/templates/components/CommunityTemplateHumanReadableResource'

// Types
import {Resource} from 'src/templates/components/CommunityTemplatesInstalledList'

interface Props {
  resources: Resource[]
  stackID: string
  orgID: string
}

export const CommunityTemplatesResourceSummary: FC<Props> = ({
  resources,
  stackID,
  orgID,
}) => {
  const [summaryState, setSummaryState] = useState<'expanded' | 'collapsed'>(
    'collapsed'
  )

  const toggleText = `${resources.length} Resource${
    !!resources.length ? 's' : ''
  }`

  const handleToggleTable = (): void => {
    if (summaryState === 'expanded') {
      setSummaryState('collapsed')
    } else {
      setSummaryState('expanded')
    }
  }

  const caretClassName = classnames(
    'community-templates--resources-table-caret',
    {
      'community-templates--resources-table-caret__expanded':
        summaryState === 'expanded',
    }
  )

  const tableRows = resources.map(resource => {
    switch (resource.kind) {
      case 'Bucket': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/load-data/buckets`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Check':
      case 'CheckDeadman':
      case 'CheckThreshold': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/alerting/checks/${resource.resourceID}/edit`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Dashboard': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/dashboards/${resource.resourceID}`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Label': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/settings/labels`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'NotificationEndpoint':
      case 'NotificationEndpointHTTP':
      case 'NotificationEndpointPagerDuty':
      case 'NotificationEndpointSlack': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/alerting/endpoints/${resource.resourceID}/edit`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'NotificationRule': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/alerting/rules/${resource.resourceID}/edit`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Task': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/tasks/${resource.resourceID}/edit`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Telegraf': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/load-data/telegrafs/${resource.resourceID}/view`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      case 'Variable': {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
            <td>
              <CommunityTemplateHumanReadableResource
                link={`/orgs/${orgID}/settings/variables/${resource.resourceID}/edit`}
                metaName={resource.templateMetaName}
                resourceID={resource.resourceID}
              />
            </td>
          </tr>
        )
      }
      default: {
        return (
          <tr key={`${resource.templateMetaName}-${stackID}-${resource.kind}`}>
            <td>{resource.kind}</td>
          </tr>
        )
      }
    }
  })

  return (
    <>
      <div
        className="community-templates--resources-table-toggle"
        onClick={handleToggleTable}
      >
        <Icon className={caretClassName} glyph={IconFont.CaretRight} />
        <h6>{toggleText}</h6>
      </div>
      {summaryState === 'expanded' && (
        <table className="community-templates--resources-table">
          <tbody>{tableRows}</tbody>
        </table>
      )}
    </>
  )
}
