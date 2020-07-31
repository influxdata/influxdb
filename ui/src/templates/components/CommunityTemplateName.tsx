// Libraries
import React, {FC} from 'react'

// Components
import {
  Gradients,
  Heading,
  HeadingElement,
  Panel,
  Button,
  ComponentColor,
  ComponentSize,
  FlexBox,
  FlexDirection,
  AlignItems,
  InfluxColors,
} from '@influxdata/clockface'
import CommunityTemplateNameIcon from 'src/templates/components/CommunityTemplateNameIcon'

interface Props {
  templateName: string
  buttonStatus: string
  resourceCount?: number
  resourcePlural?: string
  onClickInstall?: () => void
}

import {} from 'react'

const CommunityTemplateName: FC<Props> = ({
  templateName,
  buttonStatus,
  resourceCount,
  resourcePlural,
  onClickInstall,
}) => {
  let installButton

  if (onClickInstall && resourceCount > 0) {
    installButton = (
      <Button
        text="Install Template"
        color={ComponentColor.Success}
        size={ComponentSize.Medium}
        onClick={onClickInstall}
        status={buttonStatus}
      />
    )
  }

  if (resourceCount > 1) {
    resourcePlural = 'resources'
  } else {
    resourcePlural = 'resource'
  }

  return (
    <Panel border={true} gradient={Gradients.SpirulinaSmoothie}>
      <Panel.Body
        margin={ComponentSize.Large}
        direction={FlexDirection.Row}
        alignItems={AlignItems.Center}
      >
        <CommunityTemplateNameIcon
          strokeWidth={2}
          strokeColor={InfluxColors.Neutrino}
          width={54}
          height={54}
        />
        <FlexBox.Child grow={1} shrink={0}>
          <Heading
            className="community-templates--template-name"
            element={HeadingElement.H4}
          >
            {templateName}
          </Heading>
          <p className="community-templates--template-description">
            Installing this template will create{' '}
            <strong>
              {resourceCount} {resourcePlural}
            </strong>{' '}
            in your system
          </p>
        </FlexBox.Child>
        {installButton}
      </Panel.Body>
    </Panel>
  )
}

export default CommunityTemplateName
