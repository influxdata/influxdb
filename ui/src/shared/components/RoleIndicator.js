import React, {PropTypes} from 'react'
import uuid from 'node-uuid'
import {connect} from 'react-redux'

import ReactTooltip from 'react-tooltip'

import {getMeRole} from 'shared/reducers/helpers/auth'

const RoleIndicator = ({me, isUsingAuth}) => {
  if (!isUsingAuth) {
    return null
  }

  const roleName = getMeRole(me)

  const RoleTooltip = `<h1>Role: <code>${roleName}</code></h1>`
  const uuidTooltip = uuid.v4()

  return (
    <div
      className="role-indicator"
      data-for={uuidTooltip}
      data-tip={RoleTooltip}
    >
      <span className="icon user" />
      <ReactTooltip
        id={uuidTooltip}
        effect="solid"
        html={true}
        place="bottom"
        class="influx-tooltip"
      />
    </div>
  )
}

const {arrayOf, bool, shape, string} = PropTypes

RoleIndicator.propTypes = {
  isUsingAuth: bool.isRequired,
  me: shape({
    roles: arrayOf(
      shape({
        name: string.isRequired,
      })
    ),
  }),
}

const mapStateToProps = ({auth: {me, isUsingAuth}}) => ({
  me,
  isUsingAuth,
})

export default connect(mapStateToProps)(RoleIndicator)
