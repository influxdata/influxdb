import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {loadUsersAsync} from 'src/admin/actions'
import UsersTable from 'src/admin/components/UsersTable'
import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'src/shared/components/Tabs';

const TABS = ['Users', 'Roles'];

class UsersPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TABS[0],
    }

    this.handleActivateTab = this.handleActivateTab.bind(this)
  }

  componentDidMount() {
    const {source, loadUsers} = this.props
    loadUsers(source.links.users)
  }

  handleActivateTab(activeIndex) {
    this.setState({activeTab: TABS[activeIndex]});
  }

  render() {
    const {users} = this.props
    const {activeIndex} = this.state

    return (
      <div id="users-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <Tabs onSelect={this.handleActivateTab}>
                <TabList>
                  <Tab>{TABS[0]}</Tab>
                  <Tab>{TABS[1]}</Tab>
                </TabList>
                <TabPanels activeIndex={activeIndex}>
                  <TabPanel>
                    <UsersTable
                      users={users}
                    />
                  </TabPanel>
                  <TabPanel>
                    <UsersTable
                      users={users}
                    />
                  </TabPanel>
                </TabPanels>
              </Tabs>
            </div>
          </div>
        </div>
      </div>
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
  users: arrayOf(shape()),
  loadUsers: func,
}

const mapStateToProps = ({admin}) => ({
  users: admin.users,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(UsersPage);
