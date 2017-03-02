import React, {Component, PropTypes} from 'react'
import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'src/shared/components/Tabs';
import UsersTable from 'src/admin/components/UsersTable'
import RolesTable from 'src/admin/components/RolesTable'

const TABS = ['Users', 'Roles'];

class UsersTabs extends Component {
  constructor(props) {
    super(props)

    this.state = {
      activeTab: TABS[0],
    }

    this.handleActivateTab = this.handleActivateTab.bind(this)
  }

  handleActivateTab(activeIndex) {
    this.setState({activeTab: TABS[activeIndex]})
  }

  render() {
    const {users, roles} = this.props

    return (
      <Tabs onSelect={this.handleActivateTab}>
        <TabList>
          <Tab>{TABS[0]}</Tab>
          <Tab>{TABS[1]}</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <UsersTable
              users={users}
            />
          </TabPanel>
          <TabPanel>
            <RolesTable
              roles={roles}
            />
          </TabPanel>
        </TabPanels>
      </Tabs>
    )
  }
}

const {
  arrayOf,
  shape,
  string,
} = PropTypes

UsersTabs.propTypes = {
  users: arrayOf(shape({
    name: string.isRequired,
    roles: arrayOf(shape({
      name: string,
    })),
  })),
  roles: arrayOf(shape()),
}

export default UsersTabs
