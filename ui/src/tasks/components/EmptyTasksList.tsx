// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {EmptyState} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import {displayOverlay} from 'src/overlays/components/OverlayLink'

// Types
import {ComponentSize} from '@influxdata/clockface'

interface OwnProps {
  searchTerm: string
  onCreate: () => void
  totalCount: number
}

type Props = OwnProps & WithRouterProps

class EmptyTasksList extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate, totalCount, router, location} = this.props

    const handleOpenImportOverlay = displayOverlay(
      location.pathname,
      router,
      'import-task'
    )
    const handleOpenCreateFromTemplateOverlay = displayOverlay(
      location.pathname,
      router,
      'create-task-from-template'
    )

    if (totalCount && searchTerm === '') {
      return (
        <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
          <EmptyState.Text
            text={`All ${totalCount} of your Tasks are inactive`}
          />
        </EmptyState>
      )
    }

    if (searchTerm === '') {
      return (
        <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
          <EmptyState.Text
            text={"Looks like you don't have any Tasks , why not create one?"}
            highlightWords={['Tasks']}
          />
          <AddResourceDropdown
            canImportFromTemplate={true}
            onSelectNew={onCreate}
            onSelectImport={handleOpenImportOverlay}
            onSelectTemplate={handleOpenCreateFromTemplateOverlay}
            resourceName="Task"
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState testID="empty-tasks-list" size={ComponentSize.Large}>
        <EmptyState.Text text="No Tasks match your search term" />
      </EmptyState>
    )
  }
}

export default withRouter<OwnProps>(EmptyTasksList)
