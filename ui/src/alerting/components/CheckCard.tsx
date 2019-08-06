// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import CheckCardContext from 'src/alerting/components/CheckCardContext'

// Constants
import {DEFAULT_CHECK_NAME} from 'src/alerting/constants'

// Actions
import {updateCheck, deleteCheck} from 'src/alerting/actions/checks'

// Types
import {Check} from 'src/types'

interface DispatchProps {
  updateCheck: typeof updateCheck
  deleteCheck: typeof deleteCheck
}

interface OwnProps {
  check: Check
}

type Props = OwnProps & DispatchProps & WithRouterProps

const CheckCard: FunctionComponent<Props> = ({
  check,
  updateCheck,
  deleteCheck,
  params: {orgID},
  router,
}) => {
  const onUpdateName = (name: string) => {
    updateCheck({id: check.id, name})
  }

  const onUpdateDescription = (description: string) => {
    updateCheck({id: check.id, description})
  }

  const onDelete = () => {
    deleteCheck(check.id)
  }

  const onExport = () => {}

  const onClone = () => {}

  const onToggle = () => {
    const status = check.status === 'active' ? 'inactive' : 'active'

    updateCheck({id: check.id, status})
  }

  const onCheckClick = () => {
    router.push(`/orgs/${orgID}/alerting/checks/${check.id}`)
  }

  return (
    <ResourceCard
      key={`check-id--${check.id}`}
      testID="check-card"
      name={
        <ResourceCard.EditableName
          onUpdate={onUpdateName}
          onClick={onCheckClick}
          name={check.name}
          noNameString={DEFAULT_CHECK_NAME}
          testID="check-card--name"
          buttonTestID="check-card--name-button"
          inputTestID="check-card--input"
        />
      }
      toggle={
        <SlideToggle
          active={check.status === 'active'}
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="check-card--slide-toggle"
        />
      }
      description={
        <ResourceCard.Description
          onUpdate={onUpdateDescription}
          description={check.description}
          placeholder={`Describe ${check.name}`}
        />
      }
      // labels
      disabled={check.status === 'inactive'}
      contextMenu={
        <CheckCardContext
          onDelete={onDelete}
          onExport={onExport}
          onClone={onClone}
        />
      }
      metaData={[<>{check.updatedAt.toString()}</>]}
    />
  )
}

const mdtp: DispatchProps = {
  updateCheck: updateCheck,
  deleteCheck: deleteCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter(CheckCard))
