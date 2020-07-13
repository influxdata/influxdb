// Libraries
import React, {FC, ReactNode, useState} from 'react'
import classnames from 'classnames'

// Components
import {
  Heading,
  HeadingElement,
  Icon,
  IconFont,
  FlexBox,
  ComponentSize,
  AlignItems,
  FlexDirection,
} from '@influxdata/clockface'

interface Props {
  title: string
  count: number
  children?: ReactNode
}

const CommunityTemplateListGroup: FC<Props> = ({title, count, children}) => {
  const [mode, setMode] = useState<'expanded' | 'collapsed'>('collapsed')
  const groupClassName = classnames('community-templates--list-group', {
    [`community-templates--list-group__${mode}`]: mode,
  })

  const handleToggleMode = () => {
    if (mode === 'expanded') {
      setMode('collapsed')
    } else {
      setMode('expanded')
    }
  }

  if (!React.Children.count(children)) {
    return null
  }

  return (
    <div className={groupClassName}>
      <div
        className="community-templates--list-header"
        onClick={handleToggleMode}
      >
        <div className="community-templates--list-toggle">
          <Icon glyph={IconFont.CaretRight} />
        </div>
        <Heading element={HeadingElement.H5}>{title}</Heading>
        <Heading
          element={HeadingElement.Div}
          appearance={HeadingElement.H6}
          className="community-templates--list-counter"
        >{`(${count} selected)`}</Heading>
      </div>
      {mode === 'expanded' && (
        <FlexBox
          margin={ComponentSize.Small}
          direction={FlexDirection.Column}
          alignItems={AlignItems.Stretch}
          className="community-templates--list-group-items"
        >
          {children}
        </FlexBox>
      )}
    </div>
  )
}

export default CommunityTemplateListGroup
