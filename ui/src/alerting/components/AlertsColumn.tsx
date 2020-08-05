// Libraries
import React, {FC, ReactChild, useState} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState, ResourceType, ColumnTypes} from 'src/types'
import {LimitStatus, MonitoringLimits} from 'src/cloud/actions/limits'

// Components
import {
  Panel,
  InfluxColors,
  DapperScrollbars,
  Input,
  IconFont,
  FlexBox,
  FlexDirection,
  ComponentSize,
  QuestionMarkTooltip,
  ComponentColor,
} from '@influxdata/clockface'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import AssetLimitButton from 'src/cloud/components/AssetLimitButton'

// Utils
import {
  extractChecksLimits,
  extractRulesLimits,
  extractEndpointsLimits,
} from 'src/cloud/utils/limits'

// Constants
import {CLOUD} from 'src/shared/constants'

interface OwnProps {
  type: ColumnTypes
  title: string
  createButton: JSX.Element
  questionMarkTooltipContents: JSX.Element | string
  children: (searchTerm: string) => ReactChild
  tabIndex: number
}

interface StateProps {
  limitStatus: MonitoringLimits
}

const AlertsColumnHeader: FC<OwnProps & StateProps> = ({
  type,
  children,
  title,
  limitStatus,
  createButton,
  questionMarkTooltipContents,
  tabIndex,
}) => {
  const [searchTerm, onChangeSearchTerm] = useState('')

  const formattedTitle = title.toLowerCase().replace(' ', '-')
  const panelClassName = `alerting-index--column alerting-index--${formattedTitle}`
  const resourceName = title.substr(0, title.length - 1)

  const isLimitExceeded =
    CLOUD &&
    limitStatus[type] === LimitStatus.EXCEEDED &&
    type !== ResourceType.Checks

  const assetLimitButton = (
    <AssetLimitButton
      color={ComponentColor.Secondary}
      buttonText="Create"
      resourceName={resourceName}
    />
  )

  return (
    <Panel
      backgroundColor={InfluxColors.Kevlar}
      className={panelClassName}
      testID={`${type}--column`}
    >
      <Panel.Header>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
          <h4 style={{width: 'auto', marginRight: '6px'}}>{title}</h4>
          <QuestionMarkTooltip
            diameter={18}
            color={ComponentColor.Primary}
            testID={`${title}--question-mark`}
            tooltipContents={questionMarkTooltipContents}
          />
        </FlexBox>
        {isLimitExceeded ? assetLimitButton : createButton}
      </Panel.Header>
      <div className="alerting-index--search">
        <Input
          icon={IconFont.Search}
          placeholder={`Filter ${title}...`}
          value={searchTerm}
          onChange={e => onChangeSearchTerm(e.target.value)}
          testID={`filter--input ${type}`}
          tabIndex={tabIndex}
        />
      </div>
      <div className="alerting-index--column-body">
        <DapperScrollbars
          autoHide={true}
          style={{width: '100%', height: '100%'}}
        >
          <div className="alerting-index--list">
            {children(searchTerm)}
            <AssetLimitAlert
              resourceName={title}
              limitStatus={limitStatus[type]}
            />
          </div>
        </DapperScrollbars>
      </div>
    </Panel>
  )
}

const mstp = ({cloud: {limits}}: AppState) => {
  return {
    limitStatus: {
      [ResourceType.Checks]: extractChecksLimits(limits),
      [ResourceType.NotificationRules]: extractRulesLimits(limits),
      [ResourceType.NotificationEndpoints]: extractEndpointsLimits(limits),
    },
  }
}

export default connect(mstp)(AlertsColumnHeader)
