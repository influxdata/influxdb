// Libraries
import React, {FC, ReactChild, useState} from 'react'

// Types
import {ResourceType} from 'src/types'

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

type ColumnTypes =
  | ResourceType.NotificationRules
  | ResourceType.NotificationEndpoints
  | ResourceType.Checks

interface Props {
  type: ColumnTypes
  title: string
  createButton: JSX.Element
  questionMarkTooltipContents: JSX.Element | string
  children: (searchTerm: string) => ReactChild
}

const AlertsColumnHeader: FC<Props> = ({
  type,
  children,
  title,
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
          <div className="alerting-index--list">{children(searchTerm)}</div>
        </DapperScrollbars>
      </div>
    </Panel>
  )
}

export default AlertsColumnHeader
