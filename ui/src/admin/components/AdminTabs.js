import React, {Component, PropTypes} from 'react'
import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'src/shared/components/Tabs';
import UsersTable from 'src/admin/components/UsersTable'
import RolesTable from 'src/admin/components/RolesTable'
import QueriesPage from 'src/admin/containers/QueriesPage'

const TABS = ['Users', 'Roles', 'Queries'];

class AdminTabs extends Component {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TABS[0],
    }

    this.handleActivateTab = ::this.handleActivateTab
  }

  handleActivateTab(activeIndex) {
    this.setState({activeTab: TABS[activeIndex]})
  }

  render() {
    const {users, roles, source, addUser, addFlashMessage} = this.props

    return (
      <Tabs onSelect={this.handleActivateTab}>
        <TabList>
          <Tab>{TABS[0]}</Tab>
          <Tab>{TABS[1]}</Tab>
          <Tab>{TABS[2]}</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <UsersTable
              source={source}
              users={users}
              addUser={addUser}
              addFlashMessage={addFlashMessage}
            />
          </TabPanel>
          <TabPanel>
            <RolesTable
              roles={roles}
            />
          </TabPanel>
          <TabPanel>
            <QueriesPage source={source} />
          </TabPanel>
        </TabPanels>
      </Tabs>
    )
  }
}

const {
  arrayOf,
  func,
  shape,
  string,
} = PropTypes

AdminTabs.propTypes = {
  users: arrayOf(shape({
    name: string.isRequired,
    roles: arrayOf(shape({
      name: string,
    })),
  })),
  source: shape(),
  roles: arrayOf(shape()),
  addUser: func.isRequired,
  addFlashMessage: func.isRequired,
}

export default AdminTabs
