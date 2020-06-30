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
  InputToggleType,
  Toggle,
} from '@influxdata/clockface'

interface Props {
  title?: string
  description?: string
  children?: ReactNode
  handleToggle: () => void
  shouldInstall: boolean
}

const CommunityTemplateListItem: FC<Props> = ({
  title,
  children,
  description,
  handleToggle,
  shouldInstall = true,
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
        <Toggle
          id={`community-templates-install--${title}`}
          type={InputToggleType.Checkbox}
          onChange={handleToggle}
          size={ComponentSize.ExtraSmall}
          checked={shouldInstall}
        />
        {children}
      </Panel.Body>
    </Panel>
  )
}

export default CommunityTemplateListItem
