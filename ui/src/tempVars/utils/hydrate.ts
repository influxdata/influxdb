import {proxy} from 'src/utils/queryUrlGenerator'
import {parseMetaQuery} from 'src/tempVars/parsing'

import templateReplace, {
  templateInternalReplace,
} from 'src/tempVars/utils/replace'
import {getSelectedValue, getLocalSelectedValue} from 'src/tempVars/utils'

import {TEMPLATE_VARIABLE_TYPES} from 'src/tempVars/constants'

import {Template, TemplateValue, RemoteDataState} from 'src/types'

/*
  - Many scenarios invalidate the dependency graph
  - Many scenarios cause nodes in the graph to become stale
  - Immutable graph data structures are clunky
  - Template state is stored in Redux, which favors immutable data structures
  - The dependency graph is cheap to construct (small number of templates)
  - With a cache, the dependency graph is cheap to hydrate
*/

type TemplateName = string
type QueryResult = string

interface TemplateNode {
  parents: TemplateNode[]
  children: TemplateNode[]
  status: RemoteDataState
  initialTemplate: Template
  hydratedTemplate: Template
}

type TemplateGraph = TemplateNode[]

interface TemplateQueryFetcher {
  fetch: (query: string) => Promise<QueryResult[]>
}

interface Selections {
  [tempVar: string]: QueryResult
}

export function lexTemplateQuery(query: string): TemplateName[] {
  const names = []

  let inName = false
  let name = ''

  for (const c of query) {
    if (!inName && c === ':') {
      inName = true
      name = ':'
    } else if (inName && c === ':') {
      inName = false
      name += ':'
      names.push(name)
      name = ''
    } else if (inName && c !== ':') {
      name += c
    }
  }

  if (inName) {
    throw new Error(`malformed template query ${query}`)
  }

  return names
}

function verifyAcyclic(graph: TemplateGraph): void {
  for (const node of graph) {
    verifyAcyclicHelper(node, [])
  }
}

function verifyAcyclicHelper(node: TemplateNode, seen: TemplateNode[]) {
  if (seen.includes(node)) {
    const tempVar = node.initialTemplate.tempVar

    throw new Error(`cyclic dependency in template "${tempVar}"`)
  }

  for (const child of node.children) {
    verifyAcyclicHelper(child, [...seen, node])
  }
}

export function graphFromTemplates(templates: Template[]): TemplateGraph {
  interface NodesById {
    [id: string]: TemplateNode
  }

  const nodesById: NodesById = templates.reduce(
    (acc, t) => ({
      ...acc,
      [t.id]: {
        parents: [],
        children: [],
        status: RemoteDataState.NotStarted,
        initialTemplate: t,
        hydratedTemplate: null,
      },
    }),
    {}
  )

  const nodes = Object.values(nodesById)

  for (const template of templates) {
    if (!template.query || !template.query.influxql) {
      continue
    }

    const childNames = lexTemplateQuery(template.query.influxql)
    const nodeIsChild = n => childNames.includes(n.initialTemplate.tempVar)
    const children = nodes.filter(nodeIsChild)

    nodesById[template.id].children.push(...children)

    for (const child of children) {
      child.parents.push(nodesById[template.id])
    }
  }

  verifyAcyclic(nodes)

  return nodes
}

function findLeaves(graph: TemplateGraph): TemplateNode[] {
  return graph.filter(node => !node.children.length)
}

function isResolved(node: TemplateNode) {
  return node.status === RemoteDataState.Done
}

class CachingTemplateQueryFetcher implements TemplateQueryFetcher {
  private proxyUrl: string

  private cache: {
    [proxyUrl: string]: {
      [query: string]: QueryResult[]
    }
  }

  constructor() {
    this.cache = {}
  }

  public setProxyUrl(proxyUrl: string): CachingTemplateQueryFetcher {
    if (!proxyUrl) {
      throw new Error('Must supply proxyUrl')
    }

    this.proxyUrl = proxyUrl

    if (!this.cache[proxyUrl]) {
      this.cache[proxyUrl] = {}
    }

    return this
  }

  public async fetch(query) {
    const cached = this.cache[this.proxyUrl][query]

    if (!!cached) {
      return Promise.resolve([...cached])
    }

    const response = await proxy({source: this.proxyUrl, query})
    const values = parseMetaQuery(query, response.data)

    this.cache[this.proxyUrl][query] = values

    return [...values]
  }
}

const defaultFetcher = new CachingTemplateQueryFetcher()

interface HydrateTemplateOptions {
  selections?: Selections
  proxyUrl?: string
  fetcher?: TemplateQueryFetcher
}

export async function hydrateTemplate(
  template: Template,
  templates: Template[],
  {
    proxyUrl,
    fetcher = defaultFetcher.setProxyUrl(proxyUrl),
    selections = {},
  }: HydrateTemplateOptions
): Promise<Template> {
  if (!template.query || !template.query.influxql) {
    return Promise.resolve(template)
  }

  const query = templateReplace(templateInternalReplace(template), templates)
  const values = await fetcher.fetch(query)

  const templateValues = newTemplateValues(
    template,
    values,
    selections[template.tempVar]
  )

  return {...template, values: templateValues}
}

export function newTemplateValues(
  template: Template,
  newValues: string[],
  hopefullySelectedValue?: string
): TemplateValue[] {
  if (!newValues.length) {
    return []
  }

  const type = TEMPLATE_VARIABLE_TYPES[template.type]

  let selectedValue = getSelectedValue(template)

  if (!selectedValue || !newValues.includes(selectedValue)) {
    // The persisted selected value may no longer exist as a result for the
    // templates metaquery. In this case we select the first actual result
    selectedValue = newValues[0]
  }

  let localSelectedValue = hopefullySelectedValue

  if (!localSelectedValue) {
    localSelectedValue = getLocalSelectedValue(template)
  }

  if (!localSelectedValue || !newValues.includes(localSelectedValue)) {
    localSelectedValue = selectedValue
  }

  return newValues.map(value => {
    return {
      type,
      value,
      selected: value === selectedValue,
      localSelected: value === localSelectedValue,
    }
  })
}

export async function hydrateTemplates(
  templates: Template[],
  hydrateOptions: HydrateTemplateOptions
) {
  const graph = graphFromTemplates(templates)

  async function resolve(node: TemplateNode) {
    const resolvedTemplates = graph
      .filter(isResolved)
      .map(t => t.hydratedTemplate)

    node.status = RemoteDataState.Loading

    node.hydratedTemplate = await hydrateTemplate(
      node.initialTemplate,
      resolvedTemplates,
      hydrateOptions
    )

    node.status = RemoteDataState.Done

    const parents = node.parents
      .filter(p => p.children.every(isResolved))
      .map(resolve)

    return Promise.all(parents)
  }

  await Promise.all(findLeaves(graph).map(resolve))

  return graph.map(t => t.hydratedTemplate)
}
