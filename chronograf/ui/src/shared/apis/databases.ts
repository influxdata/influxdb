import _ from 'lodash'

import {showDatabases, showRetentionPolicies} from 'src/shared/apis/metaQuery'
import showDatabasesParser from 'src/shared/parsing/showDatabases'
import showRetentionPoliciesParser from 'src/shared/parsing/showRetentionPolicies'

import {Namespace} from 'src/types/queries'

export const getDatabasesWithRetentionPolicies = async (
  proxy: string
): Promise<Namespace[]> => {
  try {
    const {data} = await showDatabases(proxy)
    const {databases} = showDatabasesParser(data)
    const rps = await showRetentionPolicies(proxy, databases)
    const namespaces = rps.data.results.reduce((acc, result, index) => {
      const {retentionPolicies} = showRetentionPoliciesParser(result)

      const dbrp = retentionPolicies.map(rp => ({
        database: databases[index],
        retentionPolicy: rp.name,
      }))

      return [...acc, ...dbrp]
    }, [])

    const sorted = _.sortBy(namespaces, ({database}: Namespace) =>
      database.toLowerCase()
    )

    return sorted
  } catch (err) {
    console.error(err)
    return []
  }
}
