import React, {PropTypes} from 'react'

const DashboardEditHeader = ({
  dashboard,
  onSave,
}) => (
  <div className="page-header full-width">
    <div className="page-header__container">
      <div className="page-header__left">
        <input
          className="chronograf-header__editing"
          autoFocus={true}
          defaultValue={dashboard && dashboard.name}
          placeholder="Dashboard name"
        />
      </div>
      <div className="page-header__right">
        <div className="btn btn-sm btn-success" onClick={onSave}>
          Save
        </div>
      </div>
    </div>
  </div>
)

const {
  shape,
  func,
} = PropTypes

DashboardEditHeader.propTypes = {
  dashboard: shape({}),
  onSave: func.isRequired,
}

export default DashboardEditHeader
