// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import {
  ComponentSize,
  Label as LabelComponent,
  ResourceCard,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'

// Types
import {Label} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import LabelContextMenu from './LabelContextMenu'

interface Props {
  label: Label
  onClick: (labelID: string) => void
  onDelete: (labelID: string) => void
}

@ErrorHandling
export default class LabelCard extends PureComponent<Props> {
  public render() {
    const {label, onDelete} = this.props

    const labelHasDescription = !!label.properties.description

    const descriptionClassName = classnames('label-card--description', {
      'label-card--description__untitled': !labelHasDescription,
    })

    const description = labelHasDescription
      ? label.properties.description
      : 'No description'

    return (
      <ResourceCard
        testID="label-card"
        contextMenu={<LabelContextMenu label={label} onDelete={onDelete} />}
        direction={FlexDirection.Row}
        alignItems={AlignItems.Center}
      >
        <LabelComponent
          id={label.id}
          name={label.name}
          color={label.properties.color}
          description={label.properties.description}
          size={ComponentSize.Small}
          onClick={this.handleClick}
        />
        <p
          className={descriptionClassName}
          data-testid="label-card--description"
        >
          {description}
        </p>
      </ResourceCard>
    )
  }

  private handleClick = (): void => {
    const {label, onClick} = this.props

    onClick(label.id)
  }
}
