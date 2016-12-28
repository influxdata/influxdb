import React from 'react';
import {storiesOf, action, linkTo} from '@kadira/storybook';

// Stubs
import kapacitor from './stubs/kapacitor';
import source from './stubs/source';
import rule from './stubs/rule';
import query from './stubs/query';
import queryConfigs from './stubs/queryConfigs';

// Actions for Spies
import * as kapacitorActions from 'src/kapacitor/actions/view'
import * as queryActions from 'src/chronograf/actions/view';

// Components
import KapacitorRule from 'src/kapacitor/components/KapacitorRule';
import ValuesSection from 'src/kapacitor/components/ValuesSection';

const valuesSection = (trigger, range = false) => (
  <div className="rule-builder">
    <ValuesSection
      rule={rule({
        trigger,
        range,
      })}
      query={query()}
      onChooseTrigger={action('chooseTrigger')}
      onUpdateValues={action('updateRuleValues')}
    />
  </div>
);

storiesOf('ValuesSection', module)
  .add('Threshold', () => (
    valuesSection('threshold')
  ))
  .add('Threshold with Range', () => (
    valuesSection('threshold', true)
  ))
  .add('Relative', () => (
    valuesSection('relative')
  ))
  .add('Deadman', () => (
    valuesSection('deadman')
  ));

storiesOf('KapacitorRule', module)
  .add('Threshold', () => (
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
  ));
