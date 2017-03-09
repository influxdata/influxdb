import React, {PropTypes} from 'react'
import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'src/shared/components/Tabs';
import UsersTable from 'src/admin/components/UsersTable'
import RolesTable from 'src/admin/components/RolesTable'
import QueriesPage from 'src/admin/containers/QueriesPage'

const AdminTabs = ({
  users,
  roles,
  source,
  isEditingUsers,
  onClickCreate,
  onEditUser,
  onSaveUser,
  onCancelEdit,
  addFlashMessage,
  onDeleteRole,
  onDeleteUser,
  onFilterRoles,
  onFilterUsers,
}) => {
  const hasRoles = !!source.links.roles

  let tabs = [
    {
      type: 'Users',
      component: (<UsersTable
        users={users}
        hasRoles={hasRoles}
        isEditingUsers={isEditingUsers}
        onClickCreate={onClickCreate}
        onEdit={onEditUser}
        onSave={onSaveUser}
        onCancel={onCancelEdit}
        addFlashMessage={addFlashMessage}
        onDelete={onDeleteUser}
        onFilter={onFilterUsers}
      />),
    },
    {
      type: 'Roles',
      component: (<RolesTable roles={roles} allUsers={users} onDelete={onDeleteRole} onFilter={onFilterRoles} />),
    },
    {
      type: 'Queries',
      component: (<QueriesPage source={source} />),
    },
  ]

  if (!hasRoles) {
    tabs = tabs.filter(t => t.type !== 'Roles')
  }

  return (
    <Tabs className="row">
      <TabList customClass="col-md-3 admin-tabs">
        {
          tabs.map((t, i) => (<Tab key={tabs[i].type}>{tabs[i].type}</Tab>))
        }
      </TabList>
      <TabPanels customClass="col-md-9">
        {
          tabs.map((t, i) => (<TabPanel key={tabs[i].type}>{t.component}</TabPanel>))
        }
      </TabPanels>
    </Tabs>
  )
}

const {
  arrayOf,
  bool,
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
  roles: arrayOf(shape()),
  source: shape(),
  isEditingUsers: bool,
  onClickCreate: func.isRequired,
  onEditUser: func.isRequired,
  onSaveUser: func.isRequired,
  onCancelEdit: func.isRequired,
  addFlashMessage: func.isRequired,
  onDeleteRole: func.isRequired,
  onDeleteUser: func.isRequired,
  onFilterRoles: func.isRequired,
  onFilterUsers: func.isRequired,
}

export default AdminTabs
