// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList} from 'src/clockface'

// Constants
import {DEFAULT_CHECK_NAME} from 'src/alerting/constants'

// Actions
import {updateCheck} from 'src/alerting/actions/checks'

// Types
import {Check, CheckBase} from 'src/types'

interface DispatchProps {
  updateCheck: typeof updateCheck
}

interface OwnProps {
  check: Check
}

type Props = OwnProps & DispatchProps & WithRouterProps

const CheckCard: FunctionComponent<Props> = ({
  check,
  updateCheck,
  router,
  params: {orgID},
}) => {
  const onUpdateName = (name: string) => {
    updateCheck({name})
  }

  const onClickName = () => {
    router.push(`/orgs/${orgID}/checks/${check.id}`)
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
      disabled={check.status == CheckBase.StatusEnum.Inactive}
      updatedAt={check.updatedAt.toString()}
    />
  )
}

const mdtp: DispatchProps = {
  updateCheck: updateCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter(CheckCard))
