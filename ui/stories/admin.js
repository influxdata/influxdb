import React from 'react'
import {storiesOf, action, linkTo} from '@kadira/storybook'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'

storiesOf('MultiSelectDropdown', module)
  .add('Select Roles', () => (
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
      label={'Select Roles'}
      onApply={action('onApply')}
    />
  ))
