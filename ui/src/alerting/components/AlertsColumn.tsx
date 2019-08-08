// Libraries
import React, {FC} from 'react'

// Components
import {
  Button,
  ComponentColor,
  Panel,
  InfluxColors,
  DapperScrollbars,
  Input,
  IconFont,
} from '@influxdata/clockface'

interface Props {
  title: string
  testID?: string
  onCreate: () => void
}

const AlertsColumnHeader: FC<Props> = ({
  children,
  onCreate,
  title,
  testID = '',
}) => {
  return (
    <Panel
      backgroundColor={InfluxColors.Kevlar}
      className="alerting-index--column"
    >
      <Panel.Header title={title}>
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
