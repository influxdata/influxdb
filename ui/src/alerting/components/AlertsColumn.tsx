// Libraries
import React, {FC, ReactChild, useState} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState, ResourceType} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'

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

// Utils
import {extractMonitoringLimitStatus} from 'src/cloud/utils/limits'

type ColumnTypes =
  | ResourceType.NotificationRules
  | ResourceType.NotificationEndpoints
  | ResourceType.Checks

interface OwnProps {
  type: ColumnTypes
  title: string
  createButton: JSX.Element
  questionMarkTooltipContents: JSX.Element | string
  children: (searchTerm: string) => ReactChild
}

interface StateProps {
  limitStatus: LimitStatus
}

const AlertsColumnHeader: FC<OwnProps & StateProps> = ({
  type,
  children,
  title,
  limitStatus,
  createButton,
  questionMarkTooltipContents,
}) => {
  const [searchTerm, onChangeSearchTerm] = useState('')

  const formattedTitle = title.toLowerCase().replace(' ', '-')
  const panelClassName = `alerting-index--column alerting-index--${formattedTitle}`

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
        {createButton}
      </Panel.Header>
      <div className="alerting-index--search">
        <Input
          icon={IconFont.Search}
          placeholder={`Filter ${title}...`}
          value={searchTerm}
          onChange={e => onChangeSearchTerm(e.target.value)}
          testID={`filter--input ${type}`}
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
              limitStatus={limitStatus}
            />
          </div>
        </DapperScrollbars>
      </div>
    </Panel>
  )
}

const mstp = ({cloud: {limits}}: AppState) => {
  return {
    limitStatus: extractMonitoringLimitStatus(limits),
  }
}

export default connect(mstp)(AlertsColumnHeader)
