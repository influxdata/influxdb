// Libraries
import React, {PureComponent} from 'react'

import {
  AlignItems,
  ComponentSize,
  FlexBox,
  FlexDirection,
  Panel,
} from '@influxdata/clockface'

import {CommunityTemplateEnvReferences} from 'src/templates/components/CommunityTemplateEnvReferences'

interface OwnProps {
  resource: any
}

type Props = OwnProps

export class CommunityTemplateParameters extends PureComponent<Props> {
  render() {
    if (Object.keys(this.props.resource.envReferences).length < 1) {
      return null
    }

    return (
      <Panel className="community-templates--item">
        <Panel.Body
          key={this.props.resource.name}
          size={ComponentSize.ExtraSmall}
          alignItems={AlignItems.Center}
          direction={FlexDirection.Row}
          margin={ComponentSize.Large}
        >
          <FlexBox
            alignItems={AlignItems.FlexStart}
            direction={FlexDirection.Column}
          >
            <CommunityTemplateEnvReferences resource={this.props.resource} />
          </FlexBox>
        </Panel.Body>
      </Panel>
    )
  }
}
