import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {loadUsersAsync} from 'src/users/actions'

class UsersPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source, loadUsers} = this.props
    loadUsers(source.links.users)
  }

  render() {
    return (
      <div>Hello Users</div>
    )
  }
}

const {
  func,
  shape,
  string,
} = PropTypes

UsersPage.propTypes = {
  source: shape({
    id: string.isRequired,
    links: shape({
      users: string.isRequired,
    }),
  }).isRequired,
  users: shape(),
  loadUsers: func,
}

const mapStateToProps = ({users}) => ({
  users,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(UsersPage);
