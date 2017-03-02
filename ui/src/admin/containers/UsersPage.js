import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {loadUsersAsync} from 'src/admin/actions'
import UsersTable from 'src/admin/components/UsersTable'

class UsersPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source, loadUsers} = this.props
    loadUsers(source.links.users)
  }

  render() {
    const {users} = this.props

    return (
      <UsersTable users={users} />
    )
  }
}

const {
  arrayOf,
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
  users: arrayOf(shape()).isRequired,
  loadUsers: func,
}

const mapStateToProps = ({admin}) => ({
  users: admin.users,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(UsersPage);
