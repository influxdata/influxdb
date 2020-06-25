// Libraries
import React, {FC, ReactNode} from 'react'
import classnames from 'classnames'

// Components
import {
  Heading,
  HeadingElement,
  Panel,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'

interface Props {
  title?: string
  description?: string
  children?: ReactNode
}

const CommunityTemplateListItem: FC<Props> = ({
  title,
  children,
  description,
}) => {
  const descriptionClassName = classnames(
    'community-templates--item-description',
    {
      'community-templates--item-description__blank': !description,
    }
  )
  return (
    <Panel className="community-templates--item">
      <Panel.Body
        size={ComponentSize.ExtraSmall}
        alignItems={AlignItems.FlexStart}
      >
        {title && <Heading element={HeadingElement.H6}>{title}</Heading>}
        <p className={descriptionClassName}>
          {description || 'No description'}
        </p>
        {children}
      </Panel.Body>
    </Panel>
  )
}

export default CommunityTemplateListItem
