// Libraries
import {normalize} from 'normalizr'

// Schema
import {templateSchema, arrayOfTemplates} from 'src/schemas/templates'

// Reducer
import {templatesReducer as reducer} from 'src/templates/reducers'

// Actions
import {
  addTemplateSummary,
  populateTemplateSummaries,
} from 'src/templates/actions/creators'

// Types
import {
  CommunityTemplate,
  RemoteDataState,
  TemplateSummaryEntities,
  TemplateSummary,
} from 'src/types'

const status = RemoteDataState.Done

const templateSummary = {
  links: {
    self: '/api/v2/documents/templates/051ff6b3a8d23000',
  },
  id: '1',
  meta: {
    name: 'foo',
    type: 'dashboard',
    description: 'A template dashboard for something',
    version: '1',
  },
  labels: [],
  status,
}

const exportTemplate = {status, item: null}

const stagedCommunityTemplate: CommunityTemplate = {}

const initialState = () => ({
  stagedCommunityTemplate,
  stagedTemplateEnvReferences: {},
  communityTemplateReadmeCollection: {},
  stagedTemplateUrl: '',
  status,
  byID: {
    ['1']: templateSummary,
    ['2']: {...templateSummary, id: '2'},
  },
  allIDs: [templateSummary.id, '2'],
  exportTemplate,
  stacks: [],
})

describe('templates reducer', () => {
  it('can set the templatess', () => {
    const schema = normalize<
      TemplateSummary,
      TemplateSummaryEntities,
      string[]
    >([templateSummary], arrayOfTemplates)

    const byID = schema.entities.templates
    const allIDs = schema.result

    const actual = reducer(undefined, populateTemplateSummaries(schema))

    expect(actual.byID).toEqual(byID)
    expect(actual.allIDs).toEqual(allIDs)
  })

  it('can add a template', () => {
    const id = '3'
    const anotherTemplateSummary = {...templateSummary, id}
    const schema = normalize<TemplateSummary, TemplateSummaryEntities, string>(
      anotherTemplateSummary,
      templateSchema
    )

    const state = initialState()

    const actual = reducer(state, addTemplateSummary(schema))

    expect(actual.allIDs.length).toEqual(Number(id))
  })
})
