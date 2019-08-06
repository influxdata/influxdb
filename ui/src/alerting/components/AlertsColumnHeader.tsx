// Libraries
import React, {FC} from 'react'

// Components
import {
  Button,
  IconFont,
  ComponentSpacer,
  JustifyContent,
  AlignItems,
  FlexDirection,
} from '@influxdata/clockface'

interface Props {
  title: string
  testID?: string
  onCreate: () => void
}

const AlertsColumnHeader: FC<Props> = ({onCreate, title, testID = ''}) => {
  return (
    <ComponentSpacer
      direction={FlexDirection.Row}
      justifyContent={JustifyContent.SpaceBetween}
      alignItems={AlignItems.Center}
    >
      <div>{title}</div>
      <Button
        text="Create"
        icon={IconFont.AddCell}
        onClick={onCreate}
        testID={`alert-column--header ${testID}`}
      />
    </ComponentSpacer>
  )
}

export default AlertsColumnHeader
