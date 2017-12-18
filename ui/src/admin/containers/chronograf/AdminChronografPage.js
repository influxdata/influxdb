import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import AdminTabs from 'src/admin/components/chronograf/AdminTabs'
import FancyScrollbar from 'shared/components/FancyScrollbar'

const AdminChronografPage = ({me}) =>
  <div className="page">
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">Chronograf Admin</h1>
        </div>
      </div>
    </div>
    <FancyScrollbar className="page-contents">
      <div className="container-fluid">
        <div className="row">
          <AdminTabs me={me} />
        </div>
      </div>
    </FancyScrollbar>
  </div>

const {shape, string} = PropTypes

AdminChronografPage.propTypes = {
  me: shape({
    id: string.isRequired,
    role: string.isRequired,
    currentOrganization: shape({
      name: string.isRequired,
      id: string.isRequired,
    }),
  }).isRequired,
}

const mapStateToProps = ({auth: {me}}) => ({
  me,
})

export default connect(mapStateToProps, null)(AdminChronografPage)
