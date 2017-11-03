import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'

class UserNavBlock extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {logoutLink, customLinks, me} = this.props

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user" />
        </div>
        <div className="sidebar-menu">
          <div className="sidebar-menu--heading">
            {me.name}
          </div>
          <div className="sidebar-menu--section">Organizations</div>
          <a className="sidebar-menu--item active" href="#">
            OrganizationName1 <strong>(Viewer)</strong>
          </a>
          <a className="sidebar-menu--item" href="#">
            OrganizationName2 <strong>(Editor)</strong>
          </a>
          <a className="sidebar-menu--item" href={logoutLink}>
            Logout
          </a>
          {customLinks ? <div className="sidebar-menu--divider" /> : null}
          {customLinks
            ? <div className="sidebar-menu--section">Custom Links</div>
            : null}
          {customLinks
            ? customLinks.map((link, i) =>
                <a
                  key={i}
                  className="sidebar-menu--item"
                  href={link.url}
                  target="_blank"
                >
                  {link.name}
                </a>
              )
            : null}
          <div className="sidebar-menu--triangle" />
        </div>
      </div>
    )
  }
}

const {array, shape, string} = PropTypes

UserNavBlock.propTypes = {
  customLinks: array,
  logoutLink: string.isRequired,
  me: shape({
    name: string.isRequired,
  }),
}

const mapStateToProps = ({auth: {me}}) => ({
  me,
})

export default connect(mapStateToProps)(UserNavBlock)
