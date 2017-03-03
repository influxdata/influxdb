import React from 'react'
import {storiesOf, action, linkTo} from '@kadira/storybook'
import Center from './components/Center'

import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import Tooltip from 'shared/components/Tooltip'

storiesOf('MultiSelectDropdown', module)
  .add('Select Roles w/label', () => (
    <Center>
      <MultiSelectDropdown
        items={[
          'Admin',
          'User',
          'Chrono Girafferoo',
          'Prophet',
          'Susford',
        ]}
        selectedItems={[
          'User',
          'Chrono Girafferoo',
        ]}
        label={'Select Roles'}
        onApply={action('onApply')}
      />
    </Center>
  ))
  .add('Selected Item list', () => (
    <Center>
      <MultiSelectDropdown
        items={[
          'Admin',
          'User',
          'Chrono Giraffe',
          'Prophet',
          'Susford',
        ]}
        selectedItems={[
          'User',
          'Chrono Giraffe',
        ]}
        onApply={action('onApply')}
      />
    </Center>
  ))
  .add('0 selected items', () => (
    <Center>
      <MultiSelectDropdown
        items={[
          'Admin',
          'User',
          'Chrono Giraffe',
          'Prophet',
          'Susford',
        ]}
        selectedItems={[]}
        onApply={action('onApply')}
      />
    </Center>
  ))

storiesOf('Tooltip', module)
  .add('Delete', () => (
    <Center>
      <Tooltip tip={`Are you sure? TrashIcon`}>
        <div className="btn btn-info btn-sm">
          Delete
        </div>
      </Tooltip>
    </Center>
  ))
