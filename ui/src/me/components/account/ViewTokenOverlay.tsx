// Libraries
import React, {PureComponent} from 'react'

// Components
import {OverlayContainer, OverlayBody, OverlayHeading} from 'src/clockface'
import PermissionsWidget, {
  PermissionsWidgetMode,
  PermissionsWidgetSelection,
} from 'src/shared/components/permissionsWidget/PermissionsWidget'

// Types
import {Authorization, Permission} from 'src/api'

const {Orgs, Users, Buckets, Tasks} = Permission.ResourceEnum
const {Write, Read} = Permission.ActionEnum

export interface TestPermission {
  resource: Permission.ResourceEnum
  actions: Permission.ActionEnum[]
  id?: string
  name?: string
  orgID?: string
  orgName?: string
}

const testPerms: TestPermission[] = [
  {
    resource: Users,
    actions: [Write, Read],
  },
  {
    resource: Orgs,
    id: '1',
    name: 'myorg',
    actions: [Read],
  },
  {
    resource: Buckets,
    id: '2',
    name: 'telegraf',
    actions: [Read],
  },
  {
    resource: Tasks, // resource will be Task `task`
    name: 'task1',
    actions: [Read],
  },
  {
    resource: Tasks, // resource will be Task `task`
    id: '2',
    name: 'task1',
    actions: [Read],
  },
]

interface Props {
  auth: Authorization
  onDismissOverlay: () => void
}

const actions = [Read, Write]

export default class ViewTokenOverlay extends PureComponent<Props> {
  public render() {
    const {description} = this.props.auth
    return (
      <OverlayContainer>
        <OverlayHeading title={description} onDismiss={this.handleDismiss} />
        <OverlayBody>
          <PermissionsWidget
            mode={PermissionsWidgetMode.Read}
            heightPixels={500}
          >
            {testPerms.map((p, i) => {
              return (
                <PermissionsWidget.Section
                  key={i}
                  id={this.id(p)}
                  title={this.title(p)}
                  mode={PermissionsWidgetMode.Read}
                >
                  {actions.map((a, i) => (
                    <PermissionsWidget.Item
                      key={i}
                      id={this.itemID(p, a)}
                      label={a}
                      selected={this.selected(p, a)}
                    />
                  ))}
                </PermissionsWidget.Section>
              )
            })}
          </PermissionsWidget>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private selected = (
    permission: TestPermission,
    action: Permission.ActionEnum
  ): PermissionsWidgetSelection => {
    const isSelected = permission.actions.some(a => a === action)

    if (isSelected) {
      return PermissionsWidgetSelection.Selected
    }

    return PermissionsWidgetSelection.Unselected
  }

  private itemID = (
    permission: TestPermission,
    action: Permission.ActionEnum
  ): string => {
    return `${permission.id || permission.resource}-${action}`
  }

  private id = (permission: TestPermission): string => {
    return permission.id || permission.resource
  }

  private title = (permission): string => {
    if (permission.name) {
      return `${permission.resource}:${permission.name}`
    }

    return `${permission.resource}:*`
  }

  private handleDismiss = () => {
    this.props.onDismissOverlay()
  }
}
