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
  FlexBox,
  IconFont,
  FlexDirection,
  ComponentColor,
} from '@influxdata/clockface'

interface Props {
  title?: string
  description?: string
  children?: ReactNode
  handleToggle: () => void
  shouldInstall: boolean
  shouldDisableToggle?: boolean
}

const CommunityTemplateListItem: FC<Props> = ({
  title,
  children,
  description,
  handleToggle,
  shouldInstall = true,
  shouldDisableToggle = false,
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
        alignItems={AlignItems.Center}
        direction={FlexDirection.Row}
        margin={ComponentSize.Large}
      >
        <Toggle
          id={`community-templates-install--${title}`}
          type={InputToggleType.Checkbox}
          onChange={handleToggle}
          size={ComponentSize.Small}
          checked={shouldInstall}
          icon={IconFont.Checkmark}
          color={ComponentColor.Success}
          disabled={shouldDisableToggle}
          testID={`templates-toggle--${title}`}
        />
        <FlexBox
          alignItems={AlignItems.FlexStart}
          direction={FlexDirection.Column}
        >
          {title && <Heading element={HeadingElement.H6}>{title}</Heading>}
          <p className={descriptionClassName}>
            {description || 'No description'}
          </p>
          {children}
        </FlexBox>
      </Panel.Body>
    </Panel>
  )
}

export default CommunityTemplateListItem
