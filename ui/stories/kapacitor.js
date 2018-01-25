import React from 'react'
import {storiesOf, action, linkTo} from '@kadira/storybook'

import {spyActions} from './shared'

// Stubs
import kapacitor from './stubs/kapacitor'
import source from './stubs/source'
import rule from './stubs/rule'
import query from './stubs/query'
import queryConfigs from './stubs/queryConfigs'

// Actions for Spies
import * as kapacitorActions from 'src/kapacitor/actions/view'
import * as queryActions from 'src/data_explorer/actions/view'

// Components
import KapacitorRule from 'src/kapacitor/components/KapacitorRule'
import ValuesSection from 'src/kapacitor/components/ValuesSection'

const valuesSection = (trigger, values) =>
  <div className="rule-builder">
    <ValuesSection
      rule={rule({
        trigger,
        values,
      })}
      query={query()}
      onChooseTrigger={action('chooseTrigger')}
      onUpdateValues={action('updateRuleValues')}
    />
  </div>

storiesOf('ValuesSection', module)
  .add('Threshold', () =>
    valuesSection('threshold', {
      operator: 'less than',
      rangeOperator: 'greater than',
      value: '10',
      rangeValue: '20',
    })
  )
  .add('Threshold inside Range', () =>
    valuesSection('threshold', {
      operator: 'inside range',
      rangeOperator: 'greater than',
      value: '10',
      rangeValue: '20',
    })
  )
  // .add('Threshold outside of Range', () => (
  //   valuesSection('threshold', {
  //     "operator": "otuside of range",
  //     "rangeOperator": "less than",
  //     "value": "10",
  //     "rangeValue": "20",
  //   })
  // ))
  .add('Relative', () =>
    valuesSection('relative', {
      change: 'change',
      operator: 'greater than',
      shift: '1m',
      value: '10',
    })
  )
  .add('Deadman', () =>
    valuesSection('deadman', {
      period: '10m',
    })
  )

storiesOf('KapacitorRule', module).add('Threshold', () =>
  <div className="chronograf-root">
    <KapacitorRule
      source={source()}
      rule={rule({
        trigger: 'threshold',
      })}
      query={query()}
      queryConfigs={queryConfigs()}
      kapacitor={kapacitor()}
      queryActions={spyActions(queryActions)}
      kapacitorActions={spyActions(kapacitorActions)}
      addFlashMessage={action('addFlashMessage')}
      enabledAlerts={['slack']}
      isEditing={true}
      router={{
        push: action('route'),
      }}
    />
  </div>
)
