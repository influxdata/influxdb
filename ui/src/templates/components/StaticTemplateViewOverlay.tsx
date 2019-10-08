import React, {PureComponent} from 'react'
import {get} from 'lodash'

// Components
import ViewOverlay from 'src/shared/components/ViewOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {RemoteDataState} from '@influxdata/clockface'

import {staticTemplates} from 'src/templates/constants/defaultTemplates'
import {DashboardTemplate} from 'src/types'

interface Props {
  templateID: string
  onDismiss: () => void
}

@ErrorHandling
class TemplateExportOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props

    return (
      <ViewOverlay
        resource={this.template}
        overlayHeading={this.overlayTitle}
        onDismissOverlay={onDismiss}
        status={RemoteDataState.Done}
      />
    )
  }

  private get template(): DashboardTemplate {
    const {templateID} = this.props

    return staticTemplates[templateID]
  }

  private get overlayTitle(): string {
    const {templateID} = this.props

    const template: DashboardTemplate = staticTemplates[templateID]

    return get(template, 'meta.name', 'View')
  }
}

export default TemplateExportOverlay
