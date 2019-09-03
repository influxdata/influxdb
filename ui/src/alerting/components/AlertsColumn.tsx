// Libraries
import React, {FC, ReactChild} from 'react'

// Components
import {
  Button,
  ComponentColor,
  Panel,
  InfluxColors,
  DapperScrollbars,
  Input,
  IconFont,
  FlexBox,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import QuestionMarkTooltip from 'src/shared/components/question_mark_tooltip/QuestionMarkTooltip'

interface Props {
  title: string
  testID?: string
  onCreate: () => void
  questionMarkTooltipContents: ReactChild
}

const AlertsColumnHeader: FC<Props> = ({
  children,
  onCreate,
  title,
  testID = '',
  questionMarkTooltipContents,
}) => {
  return (
    <Panel
      backgroundColor={InfluxColors.Kevlar}
      className="alerting-index--column"
    >
      <Panel.Header>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
          <Panel.Title style={{fontSize: '17px', width: 'auto'}}>
            {title}
          </Panel.Title>
          <QuestionMarkTooltip
            testID={`${testID}--question-mark`}
            tipContent={questionMarkTooltipContents}
          />
        </FlexBox>
        <Button
          text="Create"
          icon={IconFont.Plus}
          onClick={onCreate}
          color={ComponentColor.Primary}
          testID={`alert-column--header ${testID}`}
        />
      </Panel.Header>
      <div className="alerting-index--search">
        <Input
          icon={IconFont.Search}
          placeholder={`Filter ${title}...`}
          value=""
          onChange={() => {}}
        />
      </div>
      <div className="alerting-index--column-body">
        <DapperScrollbars
          autoSize={false}
          autoHide={true}
          style={{width: '100%', height: '100%'}}
        >
          <div className="alerting-index--list">{children}</div>
        </DapperScrollbars>
      </div>
    </Panel>
  )
}

export default AlertsColumnHeader
