import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// APIs
import {client} from 'src/utils/api'

interface Props extends WithRouterProps {
  params: {orgID: string}
}

class OrgTaskImportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Task"
        onSubmit={this.handleImportTask}
      />
    )
  }

  private onDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    // fetch tasks

    router.push(`/organizations/${orgID}/tasks`)
  }

  private handleImportTask = async (importString: string): Promise<void> => {
    // const {
    //   params: {orgID},
    // } = this.props

    try {
      const template = JSON.parse(importString)

      // convertTemplateToTask
      console.log(template)

      if (_.isEmpty(template)) {
        this.onDismiss()
      }

      client.tasks.create('org', template.script) // this should be the create with orgID.
      this.onDismiss()
    } catch (error) {}
  }
}

export default withRouter(OrgTaskImportOverlay)
