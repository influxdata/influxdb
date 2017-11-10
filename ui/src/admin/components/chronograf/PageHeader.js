import React, {PropTypes} from 'react'

const PageHeader = ({currentOrganization}) =>
  <div className="page-header">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">
          {currentOrganization.name}
        </h1>
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

PageHeader.propTypes = {
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
}

export default PageHeader
