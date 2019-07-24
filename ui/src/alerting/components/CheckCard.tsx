// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList} from 'src/clockface'
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import CheckCardContext from 'src/alerting/components/CheckCardContext'

// Constants
import {DEFAULT_CHECK_NAME} from 'src/alerting/constants'

// Actions
import {updateCheck, deleteCheck} from 'src/alerting/actions/checks'

// Types
import {Check, CheckBase} from 'src/types'

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
  router,
  params: {orgID},
}) => {
  const onUpdateName = (name: string) => {
    updateCheck({id: check.id, name})
  }

  const onClickName = () => {
    router.push(`/orgs/${orgID}/checks/${check.id}`)
  }

  const onDelete = () => {
    deleteCheck(check.id)
  }

  const onExport = () => {}

  const onClone = () => {}

  const onToggle = () => {
    const status =
      check.status == CheckBase.StatusEnum.Active
        ? CheckBase.StatusEnum.Inactive
        : CheckBase.StatusEnum.Active
    updateCheck({id: check.id, status})
  }

  return (
    <ResourceList.Card
      key={`check-id--${check.id}`}
      testID="check-card"
      name={() => (
        <ResourceList.EditableName
          onUpdate={onUpdateName}
          onClick={onClickName}
          name={check.name}
          noNameString={DEFAULT_CHECK_NAME}
          parentTestID="check-card--name"
          buttonTestID="check-card--name-button"
          inputTestID="check-card--input"
        />
      )}
      toggle={() => (
        <SlideToggle
          active={check.status == CheckBase.StatusEnum.Active}
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="check-card--slide-toggle"
        />
      )}
      // description
      // labels
      disabled={check.status == CheckBase.StatusEnum.Inactive}
      contextMenu={() => (
        <CheckCardContext
          onDelete={onDelete}
          onExport={onExport}
          onClone={onClone}
        />
      )}
      updatedAt={check.updatedAt.toString()}
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
