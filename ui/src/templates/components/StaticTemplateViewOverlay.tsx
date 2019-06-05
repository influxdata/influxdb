import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import ViewOverlay from 'src/shared/components/ViewOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {RemoteDataState} from '@influxdata/clockface'
import {DocumentCreate} from '@influxdata/influx'

import {staticTemplates} from 'src/templates/constants/defaultTemplates'

interface OwnProps {
  params: {id: string}
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class TemplateExportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ViewOverlay
        resource={this.template}
        overlayHeading={this.overlayTitle}
        onDismissOverlay={this.onDismiss}
        status={RemoteDataState.Done}
      />
    )
  }

  private get template(): DocumentCreate {
    const {
      params: {id},
    } = this.props

    return staticTemplates[id]
  }

  private get overlayTitle() {
    return this.template.meta.name
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter<Props>(TemplateExportOverlay)
